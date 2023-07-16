package agnostic

import (
	"fmt"
	"hash/maphash"
	"unsafe"
)

type Index interface {
	Truncate()
	Add(*Tuple)
	CanUse( /* predicate */ ) bool
	Name() string
}

type BTreeIndex struct {
}

type HashIndex struct {
	name  string
	attrs []int
	m     map[uint64]uintptr

	maphash.Hash
}

func NewHashIndex(name string, attrs []int) *HashIndex {
	h := &HashIndex{
		name:  name,
		attrs: attrs,
		m:     make(map[uint64]uintptr),
	}
	h.SetSeed(maphash.MakeSeed())
	return h
}

func (h *HashIndex) Name() string {
	return h.name
}

func (h *HashIndex) Add(t *Tuple) {
	for i, _ := range h.attrs {
		h.Write([]byte(fmt.Sprintf("%v", t.values[i])))
	}
	sum := h.Sum64()
	h.Reset()
	h.m[sum] = uintptr(unsafe.Pointer(t))
}

func (h *HashIndex) Truncate() {
	h.m = make(map[uint64]uintptr)
}

func (h *HashIndex) CanUse() bool {
	return false
}
