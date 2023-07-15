package agnostic

type Index interface {
	Truncate()
}

type BTreeIndex struct {
}

type HashIndex struct {
}

func NewIndex() *Index {
	return nil
}
