package main

import (
	"context"
	"database/sql"
	"net/http"
	"strings"
)

// マッチング候補となる椅子。位置情報がまだ無い椅子は Latitude/Longitude が NULL になる
type matchingChair struct {
	ID        string        `db:"id"`
	Speed     int           `db:"speed"`
	Latitude  sql.NullInt64 `db:"latitude"`
	Longitude sql.NullInt64 `db:"longitude"`
}

// マッチングの結果（ライドと、割り当てる椅子の組）
type matchedPair struct {
	rideID  string
	chairID string
}

// マッチングの結果を、1回のUPDATEでまとめて保存するためのクエリを作る。
// 1件ずつUPDATEすると、そのたびにコミット（fsync）が走るため、件数に比例して遅くなる
func buildAssignChairsQuery(pairs []matchedPair) (string, []any) {
	var sb strings.Builder
	args := make([]any, 0, len(pairs)*3)
	sb.WriteString("UPDATE rides SET chair_id = CASE id")
	for _, p := range pairs {
		sb.WriteString(" WHEN ? THEN ?")
		args = append(args, p.rideID, p.chairID)
	}
	sb.WriteString(" END WHERE id IN (")
	for i, p := range pairs {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("?")
		args = append(args, p.rideID)
	}
	sb.WriteString(")")
	return sb.String(), args
}

// 位置情報がまだ無い椅子のコスト。他に候補がある限り、選ばれないようにする
const noLocationCost = 1e6

// ライドに椅子を割り当てるコスト。乗車位置までの移動時間（距離 / 速度）。
// 同じ時間なら、速い椅子を優先する
func matchCost(ride *Ride, c *matchingChair) float64 {
	if !c.Latitude.Valid || !c.Longitude.Valid {
		return noLocationCost
	}
	distance := calculateDistance(ride.PickupLatitude, ride.PickupLongitude, int(c.Latitude.Int64), int(c.Longitude.Int64))
	speed := max(c.Speed, 1)
	return float64(distance)/float64(speed) - float64(speed)*1e-9
}

// マッチングを1回実行する。待たせている順に、空いている椅子がある限り複数のライドを処理し、
// 乗車位置までの移動時間の合計が最小になるように、椅子を割り当てる。
// 同時に2つ走ると、同じライドや同じ椅子を二重に割り当ててしまうため、排他をかける
func runMatching(ctx context.Context) error {
	matchMu.Lock()
	defer matchMu.Unlock()

	// 初期化の間は、DBが作り直されているため、マッチングしない
	if matchState.initializing.Load() {
		return nil
	}

	// DBからの読み込みは、別のゴルーチン（startMatchStateReloader）が行う。読み込むまでは、何もしない
	if !matchState.isLoaded() {
		return nil
	}

	pairs := matchState.plan()
	if len(pairs) == 0 {
		return nil
	}

	query, args := buildAssignChairsQuery(pairs)
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		return err
	}
	matchState.applyAssigned(pairs)

	// 保存できてから、椅子に通知する
	for _, p := range pairs {
		chairNotifier.notify(p.chairID)
	}
	return nil
}

// このAPIは、椅子とライドをマッチングさせる。
// 通常はアプリ内のマッチャー（startMatcher）が必要なときに実行するため、定期的に叩く必要はない。外部から叩いても、同じ排他の下で動く
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	if err := runMatching(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
