package index

import (
	"bytes"
	"encoding/binary"
	"math/rand/v2"
	"sort"
	"testing"
)

func TestBTreeSplitGetRangeDuplicateAndDelete(t *testing.T) {
	tree := NewWithOrder(5)
	permutation := rand.Perm(5_000)
	for _, number := range permutation {
		key := integer(number)
		if !tree.Insert(key, integer(number+10_000)) {
			t.Fatalf("insert %d failed", number)
		}
		if number%10 == 0 {
			tree.Insert(key, integer(number+20_000))
		}
	}
	if tree.Len() != 5_500 {
		t.Fatalf("len = %d", tree.Len())
	}
	for number := 0; number < 5_000; number++ {
		values := tree.Get(integer(number))
		expected := 1
		if number%10 == 0 {
			expected = 2
		}
		if len(values) != expected {
			t.Fatalf("key %d values = %d", number, len(values))
		}
	}
	rangeValues := tree.Scan(integer(1_234), integer(1_240), true)
	keys := make([]int, 0, len(rangeValues))
	for _, pair := range rangeValues {
		keys = append(keys, int(binary.BigEndian.Uint64(pair.Key)))
	}
	if !sort.IntsAreSorted(keys) || keys[0] != 1_234 || keys[len(keys)-1] != 1_240 {
		t.Fatalf("range keys = %v", keys)
	}
	if !tree.Delete(integer(1_235), integer(11_235)) || tree.Get(integer(1_235)) != nil {
		t.Fatal("delete failed")
	}
	if tree.Delete(integer(1_235), integer(11_235)) {
		t.Fatal("duplicate delete succeeded")
	}
}

func TestBTreeCopiesCallerBuffers(t *testing.T) {
	tree := New()
	key, value := integer(1), integer(2)
	tree.Insert(key, value)
	key[0], value[0] = 99, 99
	got := tree.Get(integer(1))
	if len(got) != 1 || binary.BigEndian.Uint64(got[0]) != 2 {
		t.Fatal("caller buffer leaked")
	}
	got[0][0] = 88
	if binary.BigEndian.Uint64(tree.Get(integer(1))[0]) != 2 {
		t.Fatal("result buffer leaked")
	}
}

func TestBTreeRandomOperationsMatchOrderedSetModel(t *testing.T) {
	for seed := uint64(1); seed <= 8; seed++ {
		t.Run(string(rune('a'+seed-1)), func(t *testing.T) {
			random := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
			tree := NewWithOrder(5 + int(seed%7))
			model := make(map[[2]uint16]struct{})
			for step := 0; step < 2_000; step++ {
				pair := [2]uint16{uint16(random.Uint64() % 160), uint16(random.Uint64() % 8)}
				key, value := integer(int(pair[0])), integer(int(pair[1]))
				_, exists := model[pair]
				if random.Uint64()%3 == 0 {
					if tree.Delete(key, value) != exists {
						t.Fatalf("step %d delete mismatch", step)
					}
					delete(model, pair)
				} else {
					if tree.Insert(key, value) == exists {
						t.Fatalf("step %d insert mismatch", step)
					}
					model[pair] = struct{}{}
				}
				if step%41 == 0 {
					assertTreeMatchesModel(t, tree, model)
				}
			}
			assertTreeMatchesModel(t, tree, model)
		})
	}
}

func FuzzBTreeMatchesOrderedSetModel(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6})
	f.Add([]byte{255, 0, 1, 0, 255, 1, 2, 1})
	f.Fuzz(func(t *testing.T, operations []byte) {
		if len(operations) > 4_096 {
			operations = operations[:4_096]
		}
		tree := NewWithOrder(5)
		model := make(map[[2]uint16]struct{})
		for offset := 0; offset+2 < len(operations); offset += 3 {
			pair := [2]uint16{uint16(operations[offset]), uint16(operations[offset+1] & 15)}
			key, value := integer(int(pair[0])), integer(int(pair[1]))
			_, exists := model[pair]
			if operations[offset+2]&1 == 0 {
				if tree.Insert(key, value) == exists {
					t.Fatal("insert disagreed with model")
				}
				model[pair] = struct{}{}
			} else {
				if tree.Delete(key, value) != exists {
					t.Fatal("delete disagreed with model")
				}
				delete(model, pair)
			}
		}
		assertTreeMatchesModel(t, tree, model)
	})
}

func assertTreeMatchesModel(t *testing.T, tree *Tree, model map[[2]uint16]struct{}) {
	t.Helper()
	expected := make([]Pair, 0, len(model))
	for pair := range model {
		expected = append(expected, Pair{Key: integer(int(pair[0])), Value: integer(int(pair[1]))})
	}
	sort.Slice(expected, func(i, j int) bool {
		if comparison := bytes.Compare(expected[i].Key, expected[j].Key); comparison != 0 {
			return comparison < 0
		}
		return bytes.Compare(expected[i].Value, expected[j].Value) < 0
	})
	actual := tree.Scan(nil, nil, false)
	if tree.Len() != len(expected) || len(actual) != len(expected) {
		t.Fatalf("tree len=%d scan=%d model=%d", tree.Len(), len(actual), len(expected))
	}
	for index := range expected {
		if !bytes.Equal(actual[index].Key, expected[index].Key) || !bytes.Equal(actual[index].Value, expected[index].Value) {
			t.Fatalf("entry %d differs", index)
		}
	}
}

func integer(value int) []byte {
	result := make([]byte, 8)
	binary.BigEndian.PutUint64(result, uint64(value))
	return result
}
