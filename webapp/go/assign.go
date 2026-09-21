package main

import "math"

// コスト行列 cost（行: ライド、列: 椅子。行数 <= 列数）に対して、合計コストが最小になる割り当てを求める（ハンガリー法）。
// 返り値の i 番目は、i 行目のライドに割り当てる椅子の列番号。計算量は O(行数^2 * 列数)
func assignMinCost(cost [][]float64) []int {
	n := len(cost)
	if n == 0 {
		return nil
	}
	m := len(cost[0])

	// 添字は 1 始まり（0 は番兵）
	u := make([]float64, n+1)
	v := make([]float64, m+1)
	p := make([]int, m+1)   // p[j]: 列 j に割り当てた行
	way := make([]int, m+1) // way[j]: 列 j に至る直前の列
	for i := 1; i <= n; i++ {
		p[0] = i
		j0 := 0
		minv := make([]float64, m+1)
		for j := range minv {
			minv[j] = math.Inf(1)
		}
		used := make([]bool, m+1)
		for {
			used[j0] = true
			i0 := p[j0]
			delta := math.Inf(1)
			j1 := 0
			for j := 1; j <= m; j++ {
				if used[j] {
					continue
				}
				cur := cost[i0-1][j-1] - u[i0] - v[j]
				if cur < minv[j] {
					minv[j] = cur
					way[j] = j0
				}
				if minv[j] < delta {
					delta = minv[j]
					j1 = j
				}
			}
			for j := 0; j <= m; j++ {
				if used[j] {
					u[p[j]] += delta
					v[j] -= delta
				} else {
					minv[j] -= delta
				}
			}
			j0 = j1
			if p[j0] == 0 {
				break
			}
		}
		for {
			j1 := way[j0]
			p[j0] = p[j1]
			j0 = j1
			if j0 == 0 {
				break
			}
		}
	}

	ans := make([]int, n)
	for j := 1; j <= m; j++ {
		if p[j] != 0 {
			ans[p[j]-1] = j - 1
		}
	}
	return ans
}
