package util

const (
	HAPPENED_BEFORE = 0
	HAPPENED_AFTER = 1
	CONCURRENT
)

func happenedBefore(ts1, ts2 map[int]int) int {
	vecTs1Lesser, vecTs2Lesser := false, false

	for k, v1 := range ts1 {
		if v2, ok := ts2[k]; ok {
			if v1 < v2 {
				vecTs1Lesser = true
			} else if v2 < v1 {
				vecTs2Lesser = true
			}
		} else {
			if v1 > 0 {
				vecTs2Lesser = true
			}
		}
	}
	for k, v2 := range ts2 {
		if v1, ok := ts1[k]; ok {
			if v2 < v1 {
				vecTs2Lesser = true
			} else if v1 < v2 {
				vecTs1Lesser = true
			}
		} else {
			if v2 > 0 {
				vecTs1Lesser = true
			}
		}
	}

	// Either if both timestamps are lesser at some index or if both timestamps are equal in all indices
	if vecTs1Lesser == vecTs2Lesser {
		return CONCURRENT
	}
	if vecTs1Lesser {
		return HAPPENED_BEFORE
	}
	return HAPPENED_AFTER
}
