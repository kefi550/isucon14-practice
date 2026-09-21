package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// ライドの状態が変わったことを、SSEで待っている接続へ知らせる。
// 状態の変更はすべてこのプロセス内で行われるため、キー（椅子ID）ごとの購読者へ通知するだけでよい
type notifier struct {
	mu   sync.Mutex
	subs map[string]map[chan struct{}]struct{}
}

func newNotifier() *notifier {
	return &notifier{subs: map[string]map[chan struct{}]struct{}{}}
}

func (n *notifier) subscribe(key string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	n.mu.Lock()
	if n.subs[key] == nil {
		n.subs[key] = map[chan struct{}]struct{}{}
	}
	n.subs[key][ch] = struct{}{}
	n.mu.Unlock()

	return ch, func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		delete(n.subs[key], ch)
		if len(n.subs[key]) == 0 {
			delete(n.subs, key)
		}
	}
}

// 通知は溜めずに1つにまとめる（受け取った側は未送信の状態をすべて取り出すため）
func (n *notifier) notify(key string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for ch := range n.subs[key] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

var chairNotifier = newNotifier()

// 通知が漏れた場合でも、状態変更から3秒以内に届けるための確認間隔
const notificationSafetyInterval = 2 * time.Second

// 通知をSSEで配信する。
// fetch は、未送信の状態変更があれば1つ返し（送信済みにする）、無ければ nil を返す。
// initial が true のときは、未送信が無くても最新の状態を返す（接続直後に最新のライドの状態を即座に送るため）。
func serveSSE(w http.ResponseWriter, r *http.Request, n *notifier, key string, fetch func(ctx context.Context, initial bool) (any, error)) {
	rc := http.NewResponseController(w)
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	// nginxにレスポンスをバッファさせない
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if err := rc.Flush(); err != nil {
		return
	}

	ctx := r.Context()
	// 取りこぼさないように、最初の取得より先に購読を始める
	signal, unsubscribe := n.subscribe(key)
	defer unsubscribe()

	// 未送信の状態変更をすべて、発生した順に送る。接続を終えるべきときは false を返す
	drain := func(initial bool) bool {
		for {
			data, err := fetch(ctx, initial)
			if err != nil {
				if ctx.Err() == nil {
					slog.Error("failed to fetch notification", "error", err)
				}
				return false
			}
			if data == nil {
				return true
			}
			b, err := json.Marshal(data)
			if err != nil {
				slog.Error("failed to marshal notification", "error", err)
				return false
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
				return false
			}
			if err := rc.Flush(); err != nil {
				return false
			}
			initial = false
		}
	}

	if !drain(true) {
		return
	}

	ticker := time.NewTicker(notificationSafetyInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-signal:
		case <-ticker.C:
		}
		if !drain(false) {
			return
		}
	}
}
