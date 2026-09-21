package main

import (
	"context"
	"database/sql"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// マッチングに使う状態をメモリに持つ。
// マッチングのたびにDBを読むと、負荷が高いときに1回の実行が遅くなるため、
// 各処理（位置更新など）のイベントで更新しつつ、一定間隔でDBから読み直して、取りこぼしを直す

// 状態をDBから読み直す間隔
const matchStateReloadInterval = time.Second

// 位置のDBへの書き込みが遅れる分の余裕（locationFlushInterval より十分長くする）
const locationSnapshotSlack = 500 * time.Millisecond

// 知らない椅子へのイベントがあったときに、早めに読み直すための最短の間隔
const matchStateDirtyReloadMinInterval = 100 * time.Millisecond

type chairState struct {
	ID     string
	Name   string
	Model  string
	Speed  int
	Active bool // 配車受付中か
	HasLoc bool
	Lat    int
	Lon    int
	// ライドを持っていて、椅子がそのライドの完了（COMPLETED）を受け取るまでは埋まっている
	Busy bool

	// イベントで更新した時刻。DBの読み直しが、読み込みの開始より新しいイベントの結果を上書きしないために使う
	addedAt  time.Time
	activeAt time.Time
	busyAt   time.Time
	locAt    time.Time
}

type pendingRide struct {
	ride    *Ride
	addedAt time.Time
}

type matchStateStore struct {
	mu       sync.Mutex
	chairs   map[string]*chairState
	speeds   map[string]int // 椅子のモデル名 -> 速度
	pending  []*pendingRide // まだ椅子が割り当てられていないライド（古い順）
	loaded   bool
	loadedAt time.Time
	dirty    bool // 知らない椅子へのイベントがあったため、早めに読み直したい
	// 最近割り当てたライドとその時刻。読み込みの開始より後に割り当てたライドを、古い読み込み結果から復活させないために使う
	assigned map[string]time.Time
	// invalidate のたびに増やす。invalidate の前に読み始めた結果を、反映させないために使う
	gen uint64

	// 初期化（POST /api/initialize）の間は、マッチングを止める
	initializing atomic.Bool
}

var matchState = &matchStateStore{
	chairs:   map[string]*chairState{},
	speeds:   map[string]int{},
	assigned: map[string]time.Time{},
}

// DBから読み込んだ状態
type matchSnapshot struct {
	chairs  []snapshotChair
	busy    map[string]struct{}
	pending []*Ride
	speeds  map[string]int
}

type snapshotChair struct {
	ID        string        `db:"id"`
	Name      string        `db:"name"`
	Model     string        `db:"model"`
	Active    bool          `db:"is_active"`
	Speed     int           `db:"speed"`
	Latitude  sql.NullInt64 `db:"latitude"`
	Longitude sql.NullInt64 `db:"longitude"`
}

// DBから、マッチングに使う状態を読み込む
func loadMatchSnapshot(ctx context.Context) (*matchSnapshot, error) {
	snap := &matchSnapshot{busy: map[string]struct{}{}, speeds: map[string]int{}}

	if err := db.SelectContext(ctx, &snap.chairs, `
SELECT c.id, c.name, c.model, c.is_active, COALESCE(m.speed, 1) AS speed, l.latitude, l.longitude
FROM chairs c
       LEFT JOIN chair_models m ON m.name = c.model
       LEFT JOIN LATERAL (SELECT latitude, longitude
                          FROM chair_locations
                          WHERE chair_id = c.id
                          ORDER BY chair_id DESC, created_at DESC
                          LIMIT 1) l ON TRUE`); err != nil {
		return nil, err
	}

	// 完了していないライド（椅子への6つの状態通知がすべて済んでいないライド）を持つ椅子は空いていない。
	// 椅子は同時に1つのライドしか持たないため、椅子ごとに最新のライドだけを見ればよい
	busyIDs := []string{}
	if err := db.SelectContext(ctx, &busyIDs, `
SELECT c.id
FROM chairs c
       JOIN LATERAL (SELECT r.id FROM rides r WHERE r.chair_id = c.id ORDER BY r.updated_at DESC LIMIT 1) lr
WHERE (SELECT COUNT(s.chair_sent_at) FROM ride_statuses s WHERE s.ride_id = lr.id) < 6`); err != nil {
		return nil, err
	}
	for _, id := range busyIDs {
		snap.busy[id] = struct{}{}
	}

	if err := db.SelectContext(ctx, &snap.pending, `SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at`); err != nil {
		return nil, err
	}

	var models []ChairModel
	if err := db.SelectContext(ctx, &models, `SELECT name, speed FROM chair_models`); err != nil {
		return nil, err
	}
	for _, m := range models {
		snap.speeds[m.Name] = m.Speed
	}
	return snap, nil
}

// 読み直す必要があるか
func (ms *matchStateStore) needsReload(now time.Time) bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if !ms.loaded {
		return true
	}
	elapsed := now.Sub(ms.loadedAt)
	// 知らない椅子へのイベントがあったときは早めに読み直すが、続けて何度も読み直さない
	return elapsed >= matchStateReloadInterval || (ms.dirty && elapsed >= matchStateDirtyReloadMinInterval)
}

func (ms *matchStateStore) isLoaded() bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.loaded
}

// 読み込みを始める前に取得する世代番号
func (ms *matchStateStore) currentGen() uint64 {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.gen
}

// 読み込んだ状態を反映する。startedAt は、DBの読み込みを始めた時刻。gen は、そのとき取得した世代番号。
// 読み込みの開始より新しいイベントの結果は、上書きしない
// 読み込みの途中で invalidate されていたときは、反映せずに false を返す
func (ms *matchStateStore) applySnapshot(snap *matchSnapshot, startedAt time.Time, gen uint64) bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if gen != ms.gen {
		return false
	}

	chairs := make(map[string]*chairState, len(snap.chairs))
	for _, sc := range snap.chairs {
		_, busy := snap.busy[sc.ID]
		cs := &chairState{ID: sc.ID, Name: sc.Name, Model: sc.Model, Speed: sc.Speed, Active: sc.Active, Busy: busy}
		if sc.Latitude.Valid && sc.Longitude.Valid {
			cs.HasLoc, cs.Lat, cs.Lon = true, int(sc.Latitude.Int64), int(sc.Longitude.Int64)
		}
		if old, ok := ms.chairs[sc.ID]; ok {
			cs.addedAt = old.addedAt
			if old.activeAt.After(startedAt) {
				cs.Active, cs.activeAt = old.Active, old.activeAt
			}
			if old.busyAt.After(startedAt) {
				cs.Busy, cs.busyAt = old.Busy, old.busyAt
			}
			// 位置は、DBへの書き込みが遅れるため、読み込みの少し前のイベントも、DBの内容より新しいものとして残す
			if old.locAt.After(startedAt.Add(-locationSnapshotSlack)) {
				cs.HasLoc, cs.Lat, cs.Lon, cs.locAt = old.HasLoc, old.Lat, old.Lon, old.locAt
			}
		}
		chairs[sc.ID] = cs
	}
	// 読み込みの開始より後に追加された椅子は、残す
	for id, old := range ms.chairs {
		if _, ok := chairs[id]; !ok && old.addedAt.After(startedAt) {
			chairs[id] = old
		}
	}
	ms.chairs = chairs

	inSnapshot := make(map[string]struct{}, len(snap.pending))
	pending := make([]*pendingRide, 0, len(snap.pending))
	for _, r := range snap.pending {
		inSnapshot[r.ID] = struct{}{}
		// 読み込みの開始より後に割り当てたライドは、読み込み結果には、まだ未割り当てとして載っている
		if at, ok := ms.assigned[r.ID]; ok && at.After(startedAt) {
			continue
		}
		pending = append(pending, &pendingRide{ride: r})
	}
	// 読み込みの開始より後に作られたライドは、残す
	for _, pr := range ms.pending {
		if _, ok := inSnapshot[pr.ride.ID]; !ok && pr.addedAt.After(startedAt) {
			pending = append(pending, pr)
		}
	}
	ms.pending = pending

	if len(snap.speeds) > 0 {
		ms.speeds = snap.speeds
	}
	ms.loaded = true
	ms.loadedAt = time.Now()
	ms.dirty = false
	return true
}

// 状態を捨てる（DBを初期化したあとに、古い状態でマッチングしないため）
func (ms *matchStateStore) invalidate() {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.chairs = map[string]*chairState{}
	ms.pending = nil
	ms.assigned = map[string]time.Time{}
	ms.loaded = false
	ms.dirty = false
	ms.gen++
}

// 1回の割り当ての対象にする、待機中のライドの最大数（古い順）
const maxMatchBatch = 50

// 空いている椅子に、待機中のライドを割り当てる計画を立てる（状態は変えない）。
// 椅子が足りないときは、古いライドから順に、空いている椅子の数までを対象にする。
// 対象のライドと椅子の間で、乗車位置までの移動時間の合計が最小になる組み合わせを選ぶ
func (ms *matchStateStore) plan() []matchedPair {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if len(ms.pending) == 0 {
		return nil
	}

	candidates := make([]*matchingChair, 0, len(ms.chairs))
	for _, cs := range ms.chairs {
		if !cs.Active || cs.Busy {
			continue
		}
		c := &matchingChair{ID: cs.ID, Speed: cs.Speed}
		if cs.HasLoc {
			c.Latitude = sql.NullInt64{Int64: int64(cs.Lat), Valid: true}
			c.Longitude = sql.NullInt64{Int64: int64(cs.Lon), Valid: true}
		}
		candidates = append(candidates, c)
	}
	if len(candidates) == 0 {
		return nil
	}
	// 同じ条件の椅子があっても、実行のたびに結果が変わらないようにする
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].ID < candidates[j].ID })

	n := min(len(ms.pending), len(candidates), maxMatchBatch)
	cost := make([][]float64, n)
	for i := 0; i < n; i++ {
		cost[i] = make([]float64, len(candidates))
		for j, c := range candidates {
			cost[i][j] = matchCost(ms.pending[i].ride, c)
		}
	}
	assignment := assignMinCost(cost)

	pairs := make([]matchedPair, 0, n)
	for i, j := range assignment {
		pairs = append(pairs, matchedPair{rideID: ms.pending[i].ride.ID, chairID: candidates[j].ID, ride: ms.pending[i].ride})
	}
	return pairs
}

// DBに保存できた割り当てを、状態に反映する
func (ms *matchStateStore) applyAssigned(pairs []matchedPair) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	now := time.Now()
	assigned := make(map[string]struct{}, len(pairs))
	// 記録は、DBの読み込みにかかる時間より十分長く残せばよい
	for id, at := range ms.assigned {
		if now.Sub(at) > 30*time.Second {
			delete(ms.assigned, id)
		}
	}
	for _, p := range pairs {
		assigned[p.rideID] = struct{}{}
		ms.assigned[p.rideID] = now
		if cs, ok := ms.chairs[p.chairID]; ok {
			cs.Busy, cs.busyAt = true, now
		}
	}
	pending := ms.pending[:0]
	for _, pr := range ms.pending {
		if _, ok := assigned[pr.ride.ID]; !ok {
			pending = append(pending, pr)
		}
	}
	ms.pending = pending
}

// 以降は、各処理から呼ぶイベント。DBを更新できたあとに呼ぶ。
// 知らない椅子へのイベントは、早めの読み直しで補う

func (ms *matchStateStore) setLocation(chairID string, lat, lon int) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	cs, ok := ms.chairs[chairID]
	if !ok {
		ms.dirty = ms.loaded
		return
	}
	cs.HasLoc, cs.Lat, cs.Lon, cs.locAt = true, lat, lon, time.Now()
}

func (ms *matchStateStore) setActive(chairID string, active bool) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	cs, ok := ms.chairs[chairID]
	if !ok {
		ms.dirty = ms.loaded
		return
	}
	cs.Active, cs.activeAt = active, time.Now()
}

// 椅子がライドの完了を受け取ったので、空いた
func (ms *matchStateStore) markIdle(chairID string) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	cs, ok := ms.chairs[chairID]
	if !ok {
		ms.dirty = ms.loaded
		return
	}
	cs.Busy, cs.busyAt = false, time.Now()
}

// 椅子が登録された（配車受付は、まだ始めていない）
func (ms *matchStateStore) addChair(chairID, name, model string) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if _, ok := ms.chairs[chairID]; ok {
		return
	}
	speed, ok := ms.speeds[model]
	if !ok {
		speed = 1
	}
	ms.chairs[chairID] = &chairState{ID: chairID, Name: name, Model: model, Speed: speed, addedAt: time.Now()}
}

// ライドが作られた
func (ms *matchStateStore) addPendingRide(ride *Ride) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.pending = append(ms.pending, &pendingRide{ride: ride, addedAt: time.Now()})
}

// 配車受付中で、位置情報があり、埋まっていない椅子（GET /api/app/nearby-chairs 用）。
// 椅子は、割り当てられたライドの完了（COMPLETED）を受け取るまで、次のライドを受けられないため、
// 評価が済んでも、椅子がそれを受け取るまでは、埋まっているものとして返さない（マッチングの「空き」と同じ）
type locatedChair struct {
	ID        string
	Name      string
	Model     string
	Latitude  int
	Longitude int
}

func (ms *matchStateStore) idleLocatedChairs() []locatedChair {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	chairs := make([]locatedChair, 0, len(ms.chairs))
	for _, cs := range ms.chairs {
		if cs.Active && cs.HasLoc && !cs.Busy {
			chairs = append(chairs, locatedChair{ID: cs.ID, Name: cs.Name, Model: cs.Model, Latitude: cs.Lat, Longitude: cs.Lon})
		}
	}
	return chairs
}
