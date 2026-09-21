package main

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// マッチングの排他。同時に2つ走ると、同じライドや同じ椅子を二重に割り当ててしまう
var matchMu sync.Mutex

var matchTrigger = make(chan struct{}, 1)

const (
	// イベントを取りこぼしても、待ちが伸びないようにするための、定期の実行間隔
	matchingSafetyInterval = 150 * time.Millisecond
	// 続けて起こされたときに、1回にまとめるための最短の間隔
	matchingMinInterval = 10 * time.Millisecond
	// 読み込みが必要かどうかを、確認する間隔
	matchStateReloadCheckInterval = 50 * time.Millisecond
	// この時間を超えた実行を、遅いものとしてログに出す
	matchingSlowThreshold = 100 * time.Millisecond
)

// マッチングが必要になる出来事（ライドの作成、椅子が空いた、椅子が配車受付を始めた）のあとに呼ぶ。
// すでに起こす合図が出ているときは何もしない
func triggerMatching() {
	select {
	case matchTrigger <- struct{}{}:
	default:
	}
}

// マッチャーを起動する。合図があったとき、および定期的に、run を1つずつ順番に実行する
func startMatcher(ctx context.Context, run func(context.Context) error) {
	go func() {
		ticker := time.NewTicker(matchingSafetyInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-matchTrigger:
			case <-ticker.C:
			}

			startedAt := time.Now()
			if err := run(ctx); err != nil && ctx.Err() == nil {
				slog.Error("failed to match", "error", err)
			}
			// アクセスログには残らないため、遅い実行だけログに出す
			if elapsed := time.Since(startedAt); elapsed > matchingSlowThreshold {
				slog.Warn("slow matching", "elapsed", elapsed.Round(time.Millisecond).String())
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(matchingMinInterval):
			}
		}
	}()
}

// マッチングに使う状態を、DBから読み込むゴルーチンを起動する。
// 起動時と初期化のあと、および一定間隔で読み込む。マッチングの排他とは別に動くため、読み込みの間もマッチングは止まらない
func startMatchStateReloader(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(matchStateReloadCheckInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			// 初期化の間は、DBが作り直されているため、読み込まない
			if matchState.initializing.Load() {
				continue
			}
			startedAt := time.Now()
			if !matchState.needsReload(startedAt) {
				continue
			}

			gen := matchState.currentGen()
			snap, err := loadMatchSnapshot(ctx)
			if err != nil {
				if ctx.Err() == nil {
					slog.Error("failed to load matching state", "error", err)
				}
				continue
			}
			if matchState.applySnapshot(snap, startedAt, gen) {
				// 読み込み結果で、割り当てられるようになったライドがあるかもしれない
				triggerMatching()
			}
		}
	}()
}
