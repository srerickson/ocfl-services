package ocflite

import (
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"iter"
	"maps"
	"slices"
	"sort"
)

// DigestMap is a map of unique digests to paths
type DigestMap map[string][]string

// Paths is an iterator that yields path/digest pairs in m. The order paths are
// yielded is not defined.
func (m DigestMap) Paths() iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for d, paths := range m {
			for _, p := range paths {
				if !yield(p, d) {
					return
				}
			}
		}
	}
}

// AllPaths returns a sorted slice of all path names in the DigestMap.
func (m DigestMap) AllPaths() []string {
	pths := make([]string, 0, len(m))
	for _, paths := range m {
		pths = append(pths, paths...)
	}
	sort.Strings(pths)
	return pths
}

// Paths is an iterator that yields path/digest pairs in m. The order paths are
// yielded is not defined.
func (m DigestMap) PathMap() PathMap {
	return PathMap(maps.Collect(m.Paths()))
}

// Hash returns a sha512 of m's contents
func (m DigestMap) Hash() string {
	return m.PathMap().Hash()
}

// PathMap is a map of unique paths to digests
type PathMap map[string]string

// Paths is an iterator that yields path/digest pairs in m. The order paths are
// yielded is not defined.
func (m PathMap) DigestMap() DigestMap {
	dm := DigestMap{}
	for p, d := range m {
		dm[d] = append(dm[d], p)
	}
	return dm
}

// Hash returns a sha512 of m's contents
func (m PathMap) Hash() string {
	paths := slices.Collect(maps.Keys(m))
	slices.Sort(paths)
	hash := sha512.New()
	for _, p := range paths {
		_, err := hash.Write([]byte(p + " " + m[p] + "\n"))
		if err != nil {
			panic(fmt.Errorf("hashing digest map: %v", err))
		}
	}
	return hex.EncodeToString(hash.Sum(nil))
}
