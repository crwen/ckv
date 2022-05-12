package cache

import (
	"math/rand"
	"time"
)

const (
	cmDepth = 4
)

type cmSketch struct {
	rows [cmDepth]cmRow
	seed [cmDepth]uint64
	mask uint64
}

func newCmSketch(numCounters int64) *cmSketch {
	if numCounters == 0 {
		panic("cmSketch: invalid numCounters")
	}
	numCounters = next2Power(numCounters)
	sketch := &cmSketch{mask: uint64(numCounters - 1)}
	source := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < cmDepth; i++ {
		sketch.seed[i] = source.Uint64()
		sketch.rows[i] = newCmRow(numCounters)
	}
	return sketch
}

// Increment  increase counter
func (s cmSketch) Increment(hashed uint64) {
	for i := range s.rows {
		// mask 01111...1111
		//    & hash          ==> index in the rows
		s.rows[i].increment((hashed ^ s.seed[i]) & s.mask)
	}
}

// Estimate estimate frequency
func (s *cmSketch) Estimate(hashed uint64) int64 {
	min := byte(255)
	for i := range s.rows {
		val := s.rows[i].get((hashed ^ s.seed[i]) & s.mask)
		if val < min {
			min = val
		}
	}

	return int64(min)
}

// Reset halves all counter values.
func (s *cmSketch) Reset() {
	for _, r := range s.rows {
		r.reset()
	}
}

// Clear zeroes all counters.
func (s *cmSketch) Clear() {
	for _, r := range s.rows {
		r.clear()
	}
}

func next2Power(x int64) int64 {
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32
	x++
	return x
}

type cmRow []byte

func newCmRow(numCounters int64) cmRow {
	return make(cmRow, numCounters/2)
}

// increment increase counter in cmRow
func (r cmRow) increment(n uint64) {
	i := n / 2
	// odd: 4   event: 0
	s := (n & 1) * 4
	// 0x77    0111, 0111

	v := (r[i] >> s) & 0x0f
	if v < 15 {
		r[i] += 1 << s
	}
	// if counter is 15, no need to increase
}

// get get frequency of n
func (r cmRow) get(n uint64) byte {
	return r[n/2] >> ((n & 1) * 4) & 0x0f
}

// reset halves all counter values.
func (r cmRow) reset() {
	for i := range r {
		r[i] = (r[i] >> 1) & 0x77
	}
}

// clear reset all counter to 0
func (r cmRow) clear() {
	for i := range r {
		r[i] = 0
	}
}
