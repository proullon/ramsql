package agnostic

type Index interface {
	Truncate()
	Add(*Tuple)
}

type BTreeIndex struct {
}

type HashIndex struct {
}

func NewIndex() *Index {
	return nil
}
