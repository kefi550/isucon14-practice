package main

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
)

// 椅子ごとの「今のライド」（最新のライド）と、そのライドの最新のステータスをメモリに持つ。
// 位置更新（POST /api/chair/coordinate）のたびに、DBから読まなくても、到着（PICKUP、ARRIVED）を判定できるようにするため。
// ライドやステータスが変わる処理（マッチング、椅子のステータス更新、位置更新、評価）が、更新のたびに反映する。
// 起動時と初期化のあとに、DBから読み込む

type chairRide struct {
	RideID               string
	UserID               string
	PickupLatitude       int
	PickupLongitude      int
	DestinationLatitude  int
	DestinationLongitude int
	Status               string
}

type chairRideStore struct {
	mu     sync.Mutex
	rides  map[string]*chairRide // 椅子ID -> 今のライド。ライドが1つも無い椅子は持たない
	loaded bool
}

var chairRides = &chairRideStore{rides: map[string]*chairRide{}}

func (s *chairRideStore) isLoaded() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loaded
}

// 椅子の今のライドを返す（コピー）
func (s *chairRideStore) get(chairID string) (chairRide, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rides[chairID]
	if !ok {
		return chairRide{}, false
	}
	return *r, true
}

// 椅子にライドを割り当てた（最初のステータスは MATCHING）
func (s *chairRideStore) assign(chairID string, ride *Ride) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rides[chairID] = &chairRide{
		RideID:               ride.ID,
		UserID:               ride.UserID,
		PickupLatitude:       ride.PickupLatitude,
		PickupLongitude:      ride.PickupLongitude,
		DestinationLatitude:  ride.DestinationLatitude,
		DestinationLongitude: ride.DestinationLongitude,
		Status:               "MATCHING",
	}
}

// 椅子の今のライドが rideID のとき、ステータスを変える。今のライドが違う（知らない）ときは、何もせず false を返す
func (s *chairRideStore) setStatus(chairID, rideID, status string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rides[chairID]
	if !ok || r.RideID != rideID {
		return false
	}
	r.Status = status
	return true
}

// 椅子の今のライドが rideID で、ステータスが from のときだけ、to に変える。
// 同じ椅子から位置更新が重なっても、PICKUP や ARRIVED を二重に追加しないための判定
func (s *chairRideStore) transition(chairID, rideID, from, to string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rides[chairID]
	if !ok || r.RideID != rideID || r.Status != from {
		return false
	}
	r.Status = to
	return true
}

// 状態を捨てる（DBを初期化するとき）
func (s *chairRideStore) invalidate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rides = map[string]*chairRide{}
	s.loaded = false
}

type chairRideRow struct {
	ChairID              string         `db:"chair_id"`
	RideID               string         `db:"ride_id"`
	UserID               string         `db:"user_id"`
	PickupLatitude       int            `db:"pickup_latitude"`
	PickupLongitude      int            `db:"pickup_longitude"`
	DestinationLatitude  int            `db:"destination_latitude"`
	DestinationLongitude int            `db:"destination_longitude"`
	Status               sql.NullString `db:"status"`
}

const chairRideQuery = `
SELECT lr.chair_id, lr.id AS ride_id, lr.user_id,
       lr.pickup_latitude, lr.pickup_longitude, lr.destination_latitude, lr.destination_longitude,
       (SELECT s.status FROM ride_statuses s WHERE s.ride_id = lr.id ORDER BY s.ride_id DESC, s.created_at DESC LIMIT 1) AS status
FROM chairs c
       JOIN LATERAL (SELECT r.* FROM rides r WHERE r.chair_id = c.id ORDER BY r.updated_at DESC LIMIT 1) lr`

func (row *chairRideRow) toChairRide() *chairRide {
	return &chairRide{
		RideID:               row.RideID,
		UserID:               row.UserID,
		PickupLatitude:       row.PickupLatitude,
		PickupLongitude:      row.PickupLongitude,
		DestinationLatitude:  row.DestinationLatitude,
		DestinationLongitude: row.DestinationLongitude,
		Status:               row.Status.String,
	}
}

// 全ての椅子の今のライドを、DBから読み込む。トラフィックが無い間（起動時、初期化の直後）に呼ぶ
func loadChairRides(ctx context.Context) error {
	rows := []chairRideRow{}
	if err := db.SelectContext(ctx, &rows, chairRideQuery); err != nil {
		return err
	}
	rides := make(map[string]*chairRide, len(rows))
	for i := range rows {
		rides[rows[i].ChairID] = rows[i].toChairRide()
	}
	chairRides.mu.Lock()
	defer chairRides.mu.Unlock()
	chairRides.rides = rides
	chairRides.loaded = true
	return nil
}

// 1台の椅子の今のライドを、DBから読み直す。状態が食い違ったときに補う
func refreshChairRide(ctx context.Context, chairID string) {
	rows := []chairRideRow{}
	if err := db.SelectContext(ctx, &rows, chairRideQuery+" WHERE c.id = ?", chairID); err != nil {
		slog.Error("failed to refresh chair ride", "chair_id", chairID, "error", err)
		return
	}
	chairRides.mu.Lock()
	defer chairRides.mu.Unlock()
	if len(rows) == 0 {
		delete(chairRides.rides, chairID)
		return
	}
	chairRides.rides[chairID] = rows[0].toChairRide()
}

// 位置更新で、到着（PICKUP、ARRIVED）を判定する。ステータスを追加すべきときは、(今のステータス, 追加するステータス, true) を返す。
// 乗車位置への到着（ENROUTE のとき）と、目的地への到着（CARRYING のとき）は、今のステータスが違うので、同時には成り立たない
func decideArrival(r chairRide, latitude, longitude int) (from, to string, ok bool) {
	if r.Status == "COMPLETED" || r.Status == "CANCELED" {
		return "", "", false
	}
	if latitude == r.PickupLatitude && longitude == r.PickupLongitude && r.Status == "ENROUTE" {
		return "ENROUTE", "PICKUP", true
	}
	if latitude == r.DestinationLatitude && longitude == r.DestinationLongitude && r.Status == "CARRYING" {
		return "CARRYING", "ARRIVED", true
	}
	return "", "", false
}
