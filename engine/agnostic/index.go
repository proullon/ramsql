package agnostic

import (
	"fmt"
	"hash/maphash"
	"unsafe"

	"github.com/proullon/ramsql/engine/log"
)

type Index interface {
	Truncate()
	Add(*Tuple)
	Name() string
	CanSourceWith(p Predicate) (bool, int64)
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

func (h *HashIndex) Add(t *Tuple) {
	for _, idx := range h.attrs {
		log.Debug("HashIndex.Add(%s): appending %s", h, fmt.Sprintf("%v", t.values[idx]))
		h.Write([]byte(fmt.Sprintf("%v", t.values[idx])))
	}
	sum := h.Sum64()
	log.Debug("HashIndex.Add(%s): %d  for %v int %v", h, sum, h.attrs, t.values)
	h.Reset()
	h.m[sum] = uintptr(unsafe.Pointer(t))
}

func (h *HashIndex) Get(values []any) (*Tuple, error) {
	for _, v := range values {
		log.Debug("HashIndex.Get(%s): appending %s", h, fmt.Sprintf("%v", v))
		h.Write([]byte(fmt.Sprintf("%v", v)))
	}
	sum := h.Sum64()
	h.Reset()

	var t *Tuple
	log.Debug("Do we have %v (-> %d) in %s ?", values, sum, h)
	ptr, ok := h.m[sum]
	if !ok {
		return nil, nil
		//		return nil, fmt.Errorf("could not find sum '%d' (%v) in index %s", sum, values, h)
	}

	t = (*Tuple)(unsafe.Pointer(ptr))
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
