package main

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// 椅子の位置情報（chair_locations）のDBへの書き込みを、後回しにしてまとめる。
// 位置更新（POST /api/chair/coordinate）は、DBへの書き込み（コミット）を待たずに応答できる。
// 椅子は位置更新の成功を確認するまで動かないため、この応答の速さが、椅子の移動の速さになる。
// 最新の位置は、メモリ（matchState）に持っているため、近くの椅子やマッチングには、すぐに反映される。
// DBへは、一定間隔でまとめて書く（要件の「3秒以内の反映」の範囲内）

const (
	// DBへ書き込む間隔
	locationFlushInterval = 50 * time.Millisecond
	// 1回のINSERTで書く最大の行数
	locationFlushBatch = 1000
	// この行数を超えたら、間隔を待たずに書き込む
	locationFlushEarlySize = 2000
	// 書き込みの失敗を、ログに出す最短の間隔
	locationErrorLogInterval = time.Second
)

type locationRow struct {
	ID        string
	ChairID   string
	Latitude  int
	Longitude int
	CreatedAt time.Time
}

type locationWriter struct {
	mu     sync.Mutex // buf と paused を守る
	buf    []locationRow
	paused bool
	wake   chan struct{}

	flushMu sync.Mutex // 書き込みを1つずつにし、pause/resume が書き込みの途中に割り込まないようにする

	exec func(ctx context.Context, query string, args ...any) error
}

var locWriter = &locationWriter{
	wake: make(chan struct{}, 1),
	exec: func(ctx context.Context, query string, args ...any) error {
		_, err := db.ExecContext(ctx, query, args...)
		return err
	},
}

// 書き込む位置を溜める。一時停止中（DBの初期化中）は捨てる
func (w *locationWriter) add(row locationRow) {
	w.mu.Lock()
	if w.paused {
		w.mu.Unlock()
		return
	}
	w.buf = append(w.buf, row)
	n := len(w.buf)
	w.mu.Unlock()

	if n >= locationFlushEarlySize {
		select {
		case w.wake <- struct{}{}:
		default:
		}
	}
}

// 溜めた位置を、DBに書き込む。書き込めなかった分は、順序を保って溜め直し、次の機会に再試行する
func (w *locationWriter) flush(ctx context.Context) error {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()

	w.mu.Lock()
	if w.paused {
		w.mu.Unlock()
		return nil
	}
	rows := w.buf
	w.buf = nil
	w.mu.Unlock()

	for len(rows) > 0 {
		n := min(len(rows), locationFlushBatch)
		if err := w.insert(ctx, rows[:n]); err != nil {
			w.mu.Lock()
			if !w.paused {
				w.buf = append(append(make([]locationRow, 0, len(rows)+len(w.buf)), rows...), w.buf...)
			}
			w.mu.Unlock()
			return err
		}
		rows = rows[n:]
	}
	return nil
}

func (w *locationWriter) insert(ctx context.Context, rows []locationRow) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO chair_locations (id, chair_id, latitude, longitude, created_at) VALUES ")
	args := make([]any, 0, len(rows)*5)
	for i, r := range rows {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(?, ?, ?, ?, ?)")
		args = append(args, r.ID, r.ChairID, r.Latitude, r.Longitude, r.CreatedAt)
	}
	return w.exec(ctx, sb.String(), args...)
}

// 一定間隔（と、溜まりすぎたとき）に書き込むゴルーチンを起動する
func (w *locationWriter) start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(locationFlushInterval)
		defer ticker.Stop()
		var lastErrLog time.Time
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			case <-w.wake:
			}
			if err := w.flush(ctx); err != nil && ctx.Err() == nil {
				if time.Since(lastErrLog) >= locationErrorLogInterval {
					slog.Error("failed to write chair locations", "error", err)
					lastErrLog = time.Now()
				}
			}
		}
	}()
}

// DBの初期化の間、書き込みを止めて、溜めた位置を捨てる（初期化の前の位置を、初期化したDBに書かないため）。
// 書き込みの途中なら、それが終わるまで待つ
func (w *locationWriter) pause() {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	w.paused = true
	w.buf = nil
}

func (w *locationWriter) resume() {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	w.paused = false
}

// 停止する前に、溜めた位置を書き切る
func (w *locationWriter) drain(ctx context.Context) {
	for i := 0; i < 5; i++ {
		if err := w.flush(ctx); err != nil {
			slog.Error("failed to drain chair locations", "error", err)
			continue
		}
		w.mu.Lock()
		n := len(w.buf)
		w.mu.Unlock()
		if n == 0 {
			return
		}
	}
}
