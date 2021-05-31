package boltedstore

type stats struct {
	s *Store
}

func (s *stats) MarshalJSON() ([]byte, error) {
	return []byte(`{}`), nil
}
