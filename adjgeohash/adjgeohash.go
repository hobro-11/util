package adjgeohash

import (
	"github.com/echoface/proximityhash"
	"github.com/mmcloughlin/geohash"
)

// 반경 radius는 미터내에 있는 geohash를 반환 (param으로 받은 geohash와 정확도가 같음)
func GetAdjacentGeohashes(gh string, radius float64) []string {
	lat, lon := geohash.Decode(gh)
	r := proximityhash.CreateGeohash(lat, lon, radius, 6)
	m := make(map[string]int)

	for _, v := range r {
		m[v] = 1
	}

	result := make([]string, 0, len(m))
	for k := range m {
		result = append(result, k)
	}
	return result
}
