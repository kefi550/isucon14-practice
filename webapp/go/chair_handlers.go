package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/oklog/ulid/v2"
)

type chairPostChairsRequest struct {
	Name               string `json:"name"`
	Model              string `json:"model"`
	ChairRegisterToken string `json:"chair_register_token"`
}

type chairPostChairsResponse struct {
	ID      string `json:"id"`
	OwnerID string `json:"owner_id"`
}

func chairPostChairs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &chairPostChairsRequest{}
	if err := bindJSON(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if req.Name == "" || req.Model == "" || req.ChairRegisterToken == "" {
		writeError(w, http.StatusBadRequest, errors.New("some of required fields(name, model, chair_register_token) are empty"))
		return
	}

	owner := &Owner{}
	if err := db.GetContext(ctx, owner, "SELECT * FROM owners WHERE chair_register_token = ?", req.ChairRegisterToken); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusUnauthorized, errors.New("invalid chair_register_token"))
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	chairID := ulid.Make().String()
	accessToken := secureRandomStr(32)

	_, err := db.ExecContext(
		ctx,
		"INSERT INTO chairs (id, owner_id, name, model, is_active, access_token) VALUES (?, ?, ?, ?, ?, ?)",
		chairID, owner.ID, req.Name, req.Model, false, accessToken,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	matchState.addChair(chairID, req.Name, req.Model)

	http.SetCookie(w, &http.Cookie{
		Path:  "/",
		Name:  "chair_session",
		Value: accessToken,
	})

	writeJSON(w, http.StatusCreated, &chairPostChairsResponse{
		ID:      chairID,
		OwnerID: owner.ID,
	})
}

type postChairActivityRequest struct {
	IsActive bool `json:"is_active"`
}

func chairPostActivity(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	chair := ctx.Value("chair").(*Chair)

	req := &postChairActivityRequest{}
	if err := bindJSON(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	_, err := db.ExecContext(ctx, "UPDATE chairs SET is_active = ? WHERE id = ?", req.IsActive, chair.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	matchState.setActive(chair.ID, req.IsActive)
	if req.IsActive {
		triggerMatching()
	}

	w.WriteHeader(http.StatusNoContent)
}

type chairPostCoordinateResponse struct {
	RecordedAt int64 `json:"recorded_at"`
}

// 椅子の最新のライドと、そのライドの最新のステータス
type chairLatestRide struct {
	Ride
	LatestStatus sql.NullString `db:"latest_status"`
}

func chairPostCoordinate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &Coordinate{}
	if err := bindJSON(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	chair := ctx.Value("chair").(*Chair)

	// 椅子は位置更新の成功を確認するまで移動しないため、応答までの往復を減らす。
	// 記録日時は読み戻さず、ここで決めた値をそのまま保存して返す
	recordedAt := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := db.ExecContext(
		ctx,
		`INSERT INTO chair_locations (id, chair_id, latitude, longitude, created_at) VALUES (?, ?, ?, ?, ?)`,
		ulid.Make().String(), chair.ID, req.Latitude, req.Longitude, recordedAt,
	); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	matchState.setLocation(chair.ID, req.Latitude, req.Longitude)

	rideStatusAdded := false
	rideUserID := ""
	if chairRides.isLoaded() {
		// ライドの状態はメモリに持っているため、DBを読まずに、到着（PICKUP、ARRIVED）を判定できる
		if cr, ok := chairRides.get(chair.ID); ok {
			rideUserID = cr.UserID
			if from, to, arrived := decideArrival(cr, req.Latitude, req.Longitude); arrived {
				// 同じ椅子から位置更新が重なっても、ステータスを二重に追加しない
				if chairRides.transition(chair.ID, cr.RideID, from, to) {
					if _, err := db.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", ulid.Make().String(), cr.RideID, to); err != nil {
						chairRides.transition(chair.ID, cr.RideID, to, from)
						writeError(w, http.StatusInternalServerError, err)
						return
					}
					rideStatusAdded = true
				}
			}
		}
	} else {
		ride := &chairLatestRide{}
		if err := db.GetContext(ctx, ride, `
SELECT r.*,
       (SELECT s.status FROM ride_statuses s WHERE s.ride_id = r.id ORDER BY s.ride_id DESC, s.created_at DESC LIMIT 1) AS latest_status
FROM rides r
WHERE r.chair_id = ?
ORDER BY r.updated_at DESC
LIMIT 1`, chair.ID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
		} else {
			rideUserID = ride.UserID
			if _, to, arrived := decideArrival(chairRide{
				Status:               ride.LatestStatus.String,
				PickupLatitude:       ride.PickupLatitude,
				PickupLongitude:      ride.PickupLongitude,
				DestinationLatitude:  ride.DestinationLatitude,
				DestinationLongitude: ride.DestinationLongitude,
			}, req.Latitude, req.Longitude); arrived {
				if _, err := db.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", ulid.Make().String(), ride.ID, to); err != nil {
					writeError(w, http.StatusInternalServerError, err)
					return
				}
				rideStatusAdded = true
			}
		}
	}

	if rideStatusAdded {
		chairNotifier.notify(chair.ID)
		userNotifier.notify(rideUserID)
	}

	writeJSON(w, http.StatusOK, &chairPostCoordinateResponse{
		RecordedAt: recordedAt.UnixMilli(),
	})
}

type simpleUser struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type chairGetNotificationResponse struct {
	Data         *chairGetNotificationResponseData `json:"data"`
	RetryAfterMs int                               `json:"retry_after_ms"`
}

type chairGetNotificationResponseData struct {
	RideID                string     `json:"ride_id"`
	User                  simpleUser `json:"user"`
	PickupCoordinate      Coordinate `json:"pickup_coordinate"`
	DestinationCoordinate Coordinate `json:"destination_coordinate"`
	Status                string     `json:"status"`
}

// true の間、椅子向け通知はSSEで配信する（false ならポーリング用のJSONを返す）
const chairNotificationSSE = true

// 椅子向けの通知データを取得する。
// 未送信の状態変更があれば、その状態を返して送信済みにする。
// 未送信が無い場合、initial なら最新の状態を、そうでなければ nil を返す。ライドがまだ1つも無い場合も nil を返す
func fetchChairNotificationData(ctx context.Context, chair *Chair, initial bool) (*chairGetNotificationResponseData, error) {
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	ride := &Ride{}
	yetSentRideStatus := RideStatus{}
	status := ""

	if err := tx.GetContext(ctx, ride, `SELECT * FROM rides WHERE chair_id = ? ORDER BY updated_at DESC LIMIT 1`, chair.ID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	if err := tx.GetContext(ctx, &yetSentRideStatus, `SELECT * FROM ride_statuses WHERE ride_id = ? AND chair_sent_at IS NULL ORDER BY created_at ASC LIMIT 1`, ride.ID); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		if !initial {
			return nil, nil
		}
		status, err = getLatestRideStatus(ctx, tx, ride.ID)
		if err != nil {
			return nil, err
		}
	} else {
		status = yetSentRideStatus.Status
	}

	user := &User{}
	if err := tx.GetContext(ctx, user, "SELECT * FROM users WHERE id = ? FOR SHARE", ride.UserID); err != nil {
		return nil, err
	}

	if yetSentRideStatus.ID != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE ride_statuses SET chair_sent_at = CURRENT_TIMESTAMP(6) WHERE id = ?`, yetSentRideStatus.ID); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	// 椅子がライドの完了を受け取ったので、次のライドを割り当てられるようになった
	if yetSentRideStatus.Status == "COMPLETED" {
		matchState.markIdle(chair.ID)
		triggerMatching()
	}

	return &chairGetNotificationResponseData{
		RideID: ride.ID,
		User: simpleUser{
			ID:   user.ID,
			Name: fmt.Sprintf("%s %s", user.Firstname, user.Lastname),
		},
		PickupCoordinate: Coordinate{
			Latitude:  ride.PickupLatitude,
			Longitude: ride.PickupLongitude,
		},
		DestinationCoordinate: Coordinate{
			Latitude:  ride.DestinationLatitude,
			Longitude: ride.DestinationLongitude,
		},
		Status: status,
	}, nil
}

func chairGetNotification(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	chair := ctx.Value("chair").(*Chair)

	if chairNotificationSSE {
		serveSSE(w, r, chairNotifier, chair.ID, func(ctx context.Context, initial bool) (any, error) {
			data, err := fetchChairNotificationData(ctx, chair, initial)
			if data == nil {
				// nil の *T を any に入れると nil にならないため、明示的に nil を返す
				return nil, err
			}
			return data, err
		})
		return
	}

	data, err := fetchChairNotificationData(ctx, chair, true)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, &chairGetNotificationResponse{
		Data:         data,
		RetryAfterMs: 30,
	})
}

type postChairRidesRideIDStatusRequest struct {
	Status string `json:"status"`
}

func chairPostRideStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rideID := r.PathValue("ride_id")

	chair := ctx.Value("chair").(*Chair)

	req := &postChairRidesRideIDStatusRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	ride := &Ride{}
	if err := tx.GetContext(ctx, ride, "SELECT * FROM rides WHERE id = ? FOR UPDATE", rideID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, errors.New("ride not found"))
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if ride.ChairID.String != chair.ID {
		writeError(w, http.StatusBadRequest, errors.New("not assigned to this ride"))
		return
	}

	switch req.Status {
	// Acknowledge the ride
	case "ENROUTE":
		if _, err := tx.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", ulid.Make().String(), ride.ID, "ENROUTE"); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	// After Picking up user
	case "CARRYING":
		status, err := getLatestRideStatus(ctx, tx, ride.ID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if status != "PICKUP" {
			writeError(w, http.StatusBadRequest, errors.New("chair has not arrived yet"))
			return
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", ulid.Make().String(), ride.ID, "CARRYING"); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	default:
		writeError(w, http.StatusBadRequest, errors.New("invalid status"))
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if req.Status == "ENROUTE" || req.Status == "CARRYING" {
		if !chairRides.setStatus(chair.ID, ride.ID, req.Status) {
			refreshChairRide(ctx, chair.ID)
		}
	}
	chairNotifier.notify(chair.ID)
	userNotifier.notify(ride.UserID)

	w.WriteHeader(http.StatusNoContent)
}
