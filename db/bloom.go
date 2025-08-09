package db

import (
	"hash/fnv"
	"math"
)

type BloomFilter struct {
	bitset []byte
	m      uint
	k      uint
}

func NewBloomFilter(n uint, fpRate float64) *BloomFilter {
	m := optimalM(n, fpRate)
	k := optimalK(n, m)

	return &BloomFilter{
		bitset: make([]byte, (m+7)/8),
		m:      m,
		k:      k,
	}
}

func (bf *BloomFilter) Add(data string) {
	for i := uint(0); i < bf.k; i++ {
		pos := bf.hash(data, i) % bf.m
		bf.bitset[pos/8] |= 1 << (pos % 8)
	}
}

func (bf *BloomFilter) MightContains(data string) bool {
	for i := uint(0); i < bf.k; i++ {
		pos := bf.hash(data, i) % bf.m
		if (bf.bitset[pos/8] & (1 << (pos % 8))) == 0 {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) hash(data string, seed uint) uint {
	h := fnv.New64a()
	h.Write([]byte{byte(seed)})
	h.Write([]byte(data))
	return uint(h.Sum64())
}

func optimalM(n uint, p float64) uint {
	return uint(math.Ceil(-float64(n) * math.Log(p) / (math.Ln2 * math.Ln2)))
}

func optimalK(n, m uint) uint {
	return uint(math.Round((float64(m) / float64(n)) * math.Ln2))
}
