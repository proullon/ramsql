package agnostic

import (
	"container/list"
	"fmt"
	"hash/maphash"
	"unsafe"
)

type IndexType int

const (
	HashIndexType IndexType = iota
	BTreeIndexType
)

type Index interface {
	Truncate()
	Add(*list.Element)
	Remove(*list.Element)
	Name() string
	CanSourceWith(p Predicate) (bool, int64)
	Get(values []any) (*list.Element, error)
}

type HashIndex struct {
	name      string
	relName   string
	relAttrs  []string
	attrs     []int
	attrsName []string
	m         map[uint64]uintptr

	maphash.Hash
}

func NewHashIndex(name string, relName string, relAttrs []Attribute, attrsName []string, attrs []int) *HashIndex {
	h := &HashIndex{
		name:      name,
		relName:   relName,
		attrs:     attrs,
		attrsName: attrsName,
		m:         make(map[uint64]uintptr),
	}
	h.SetSeed(maphash.MakeSeed())
	for _, a := range relAttrs {
		h.relAttrs = append(h.relAttrs, a.name)
	}
	return h
}

func (h *HashIndex) Name() string {
	return h.name
}

func (h *HashIndex) Add(e *list.Element) {
	t := e.Value.(*Tuple)
	for _, idx := range h.attrs {
		if t.values[idx] == nil {
			h.Write([]byte("nil"))
			continue
		}
		h.Write([]byte(fmt.Sprintf("%v", t.values[idx])))
	}
	sum := h.Sum64()
	h.Reset()
	h.m[sum] = uintptr(unsafe.Pointer(e))
}

func (h *HashIndex) Remove(e *list.Element) {
	t := e.Value.(*Tuple)
	for _, idx := range h.attrs {
		if t.values[idx] == nil {
			h.Write([]byte("nil"))
			continue
		}
		h.Write([]byte(fmt.Sprintf("%v", t.values[idx])))
	}
	sum := h.Sum64()
	h.Reset()
	delete(h.m, sum)
}

func (h *HashIndex) Get(values []any) (*list.Element, error) {
	for _, v := range values {
		if v == nil {
			h.Write([]byte("nil"))
			continue
		}
		h.Write([]byte(fmt.Sprintf("%v", v)))
	}
	sum := h.Sum64()
	h.Reset()

	var t *list.Element
	ptr, ok := h.m[sum]
	if !ok {
		return nil, nil
		//		return nil, fmt.Errorf("could not find sum '%d' (%v) in index %s", sum, values, h)
	}

	t = (*list.Element)(unsafe.Pointer(ptr))
	return t, nil
}

func (h *HashIndex) Truncate() {
	h.m = make(map[uint64]uintptr)
}

func (h *HashIndex) String() string {
	return h.Name()
}

func (h *HashIndex) CanSourceWith(p Predicate) (bool, int64) {
	if p.Relation() != h.relName {
		return false, 0
	}

	if p.Type() != Eq {
		return false, 0
	}

	var found bool
	for _, l := range h.attrsName {
		found = false
		for _, r := range p.Attribute() {
			if l == r || h.relName+"."+l == r {
				found = true
				break
			}
		}
		if !found {
			return false, 0
		}
	}

	return true, 1
}
