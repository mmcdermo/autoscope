package engine

//Utility function to merge two maps
type strMap map[string]interface{}
func (m1 strMap) Merge(m2 strMap) {
	for key, value := range m2 {
		m1[key] = value
	}
}

//Given a mapping from string-> int, return the key with the
// greatest value
func maxKey(m map[string]int64) string {
	hwms := ""
	hwmv := int64(-1)
	for k, v := range m {
		if v > hwmv {
			hwmv = v
			hwms = k
		}
	}
	return hwms
}

//Helper function to increment the value at k1 -> k2 -> v
func incrementCountMap(m map[string]map[string]int64, k1 string, k2 string) map[string]map[string]int64 {
	if _, ok := m[k1]; !ok {
		m[k1] = make(map[string]int64, 0)
	}
	if _, ok := m[k1][k2]; ok {
		m[k1][k2] += 1
	} else {
		m[k1][k2] = 1
	}
	return m
}

//Returns true if `needle` is an element of `haystack`
func listContains(haystack []string, needle string) bool {
	for _, val := range(haystack) {
		if val == needle { return true }
	}
	return false
}

//Type alias and functions to allow us to sort an array of strings by length
type ByLength []string
func (s ByLength) Len() int {
	return len(s)
}
func (s ByLength) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s ByLength) Less(i, j int) bool {
	return len(s[i]) < len(s[j])
}
