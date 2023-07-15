package agnostic

import (
	"fmt"
	"hash/maphash"
)

type Index interface {
	Truncate()
	Add(*Tuple)
	CanUse( /* predicate */ ) bool
}

type BTreeIndex struct {
}

type HashIndex struct {
	attrs []int
	m     map[uint64]any

	maphash.Hash
}

func NewHashIndex(attrs []int) *HashIndex {
	h := &HashIndex{
		attrs: attrs,
		m:     make(map[uint64]any),
	}
	h.SetSeed(maphash.MakeSeed())
	return h
}

func (h *HashIndex) Add(t *Tuple) {
	for i, _ := range h.attrs {
		h.Write([]byte(fmt.Sprintf("%v", t.values[i])))
	}
	sum := h.Sum64()
	h.Reset()
	h.m[sum] = t
}

func (h *HashIndex) Truncate() {
	h.m = make(map[uint64]any)
}

func (h *HashIndex) CanUse() bool {
	return false
}
