package main

import (
	"database/sql"
	"errors"
	"net/http"

	"github.com/jmoiron/sqlx"
)

// マッチング候補となる椅子。位置情報がまだ無い椅子は Latitude/Longitude が NULL になる
type matchingChair struct {
	ID        string        `db:"id"`
	Speed     int           `db:"speed"`
	Latitude  sql.NullInt64 `db:"latitude"`
	Longitude sql.NullInt64 `db:"longitude"`
}

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// MEMO: 最も待たせているリクエストに、乗車位置に最も早く着けそうな空いている椅子をマッチさせる
	ride := &Ride{}
	if err := db.GetContext(ctx, ride, `SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at LIMIT 1`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 配車受付中の椅子を、速度と最新の位置情報つきでまとめて取得する
	activeChairs := []matchingChair{}
	if err := db.SelectContext(ctx, &activeChairs, `
SELECT c.id, COALESCE(m.speed, 1) AS speed, l.latitude, l.longitude
FROM chairs c
       LEFT JOIN chair_models m ON m.name = c.model
       LEFT JOIN chair_locations l ON l.chair_id = c.id
                                  AND l.created_at = (SELECT MAX(created_at) FROM chair_locations WHERE chair_id = c.id)
WHERE c.is_active = TRUE`); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if len(activeChairs) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	chairIDs := make([]string, 0, len(activeChairs))
	for _, c := range activeChairs {
		chairIDs = append(chairIDs, c.ID)
	}

	// 完了していないライド（椅子への6つの状態通知がすべて済んでいないライド）を持つ椅子は空いていない
	query, args, err := sqlx.In(`
SELECT r.chair_id
FROM rides r
       JOIN ride_statuses s ON s.ride_id = r.id
WHERE r.chair_id IN (?)
GROUP BY r.id, r.chair_id
HAVING COUNT(s.chair_sent_at) < 6`, chairIDs)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	busyChairIDs := []string{}
	if err := db.SelectContext(ctx, &busyChairIDs, db.Rebind(query), args...); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	busy := make(map[string]struct{}, len(busyChairIDs))
	for _, id := range busyChairIDs {
		busy[id] = struct{}{}
	}

	// 空いている椅子のうち、乗車位置までの移動時間（距離 / 速度）が最も短いものを選ぶ
	var matched *matchingChair
	var matchedETA float64
	seen := make(map[string]struct{}, len(activeChairs))
	for i := range activeChairs {
		c := &activeChairs[i]
		if _, ok := busy[c.ID]; ok {
			continue
		}
		// 同時刻の位置情報が複数ある場合に同じ椅子が重複して返ってくるので、最初の1件だけ見る
		if _, ok := seen[c.ID]; ok {
			continue
		}
		seen[c.ID] = struct{}{}

		// 位置情報がまだ無い椅子は、他に候補がある限り後回しにする
		eta := 1e18
		if c.Latitude.Valid && c.Longitude.Valid {
			distance := calculateDistance(ride.PickupLatitude, ride.PickupLongitude, int(c.Latitude.Int64), int(c.Longitude.Int64))
			eta = float64(distance) / float64(max(c.Speed, 1))
		}
		if matched == nil || eta < matchedETA || (eta == matchedETA && c.Speed > matched.Speed) {
			matched = c
			matchedETA = eta
		}
	}
	if matched == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if _, err := db.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", matched.ID, ride.ID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
