package db

type SSTable struct {
	path string
}

func (s *SSTable) Search(key string) (string, bool) {
	if key == "" {
		return "", false
	}
	return "value_for_" + key, true
}
