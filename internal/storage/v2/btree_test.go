package v2

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestCopyOnWriteTreeBatchSplitScanAndSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tree.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var firstRoot uint64
	if err := file.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		tree, err := tx.OpenTree(0, TreeCatalog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		for index := 1999; index >= 0; index-- {
			key := []byte(fmt.Sprintf("key-%04d", index))
			value := bytes.Repeat([]byte{byte(index)}, 40)
			if err := tree.Put(key, value); err != nil {
				return DatabaseRoot{}, err
			}
		}
		firstRoot, err = tree.Flush()
		return DatabaseRoot{CommitSequence: tx.Sequence(), CatalogRoot: firstRoot, CatalogGeneration: 1, DocumentCount: 2000}, err
	}); err != nil {
		t.Fatal(err)
	}
	firstMeta := file.Meta()
	if firstRoot == 0 || firstMeta.PhysicalPageCount > 100 {
		t.Fatalf("root=%d pages=%d; batch flush wrote intermediate versions", firstRoot, firstMeta.PhysicalPageCount)
	}

	for _, index := range []int{0, 1, 999, 1999} {
		key := []byte(fmt.Sprintf("key-%04d", index))
		value, ok, err := file.TreeGet(firstRoot, TreeCatalog, key)
		if err != nil || !ok || len(value) != 40 || value[0] != byte(index) {
			t.Fatalf("get %q ok=%t len=%d err=%v", key, ok, len(value), err)
		}
	}
	if _, ok, err := file.TreeGet(firstRoot, TreeCatalog, []byte("missing")); err != nil || ok {
		t.Fatalf("missing ok=%t err=%v", ok, err)
	}
	rangeValues, err := file.TreeScan(firstRoot, TreeCatalog, []byte("key-0050"), []byte("key-0060"), 0)
	if err != nil || len(rangeValues) != 10 {
		t.Fatalf("range len=%d err=%v", len(rangeValues), err)
	}
	for index := range rangeValues {
		want := fmt.Sprintf("key-%04d", index+50)
		if string(rangeValues[index].Key) != want {
			t.Fatalf("range[%d]=%q want %q", index, rangeValues[index].Key, want)
		}
	}

	var secondRoot uint64
	if err := file.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		tree, err := tx.OpenTree(firstRoot, TreeCatalog)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if err := tree.Put([]byte("key-0999"), []byte("new-value")); err != nil {
			return DatabaseRoot{}, err
		}
		if err := tree.Put([]byte("key-2000"), []byte("inserted")); err != nil {
			return DatabaseRoot{}, err
		}
		secondRoot, err = tree.Flush()
		return DatabaseRoot{CommitSequence: tx.Sequence(), CatalogRoot: secondRoot, CatalogGeneration: 2, DocumentCount: 2001}, err
	}); err != nil {
		t.Fatal(err)
	}
	oldValue, ok, err := file.TreeGet(firstRoot, TreeCatalog, []byte("key-0999"))
	if err != nil || !ok || len(oldValue) != 40 {
		t.Fatalf("old snapshot value len=%d ok=%t err=%v", len(oldValue), ok, err)
	}
	newValue, ok, err := file.TreeGet(secondRoot, TreeCatalog, []byte("key-0999"))
	if err != nil || !ok || string(newValue) != "new-value" {
		t.Fatalf("new snapshot value=%q ok=%t err=%v", newValue, ok, err)
	}

	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	root, err := reopened.DatabaseRoot()
	if err != nil || root.CatalogRoot != secondRoot || root.DocumentCount != 2001 {
		t.Fatalf("database root=%+v err=%v", root, err)
	}
	value, ok, err := reopened.TreeGet(root.CatalogRoot, TreeCatalog, []byte("key-2000"))
	if err != nil || !ok || string(value) != "inserted" {
		t.Fatalf("reopened value=%q ok=%t err=%v", value, ok, err)
	}
	info, err := os.Stat(path)
	if err != nil || uint64(info.Size()/PageSize) != reopened.Meta().PhysicalPageCount {
		t.Fatalf("file pages=%d meta=%d err=%v", info.Size()/PageSize, reopened.Meta().PhysicalPageCount, err)
	}
}

func TestCopyOnWriteTreeDeleteMergesWithoutRebuild(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delete.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var originalRoot uint64
	if err := file.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		tree, err := tx.OpenTree(0, TreePrimary)
		if err != nil {
			return DatabaseRoot{}, err
		}
		for index := 0; index < 1000; index++ {
			if err := tree.Put([]byte(fmt.Sprintf("%04d", index)), bytes.Repeat([]byte{byte(index)}, 80)); err != nil {
				return DatabaseRoot{}, err
			}
		}
		originalRoot, err = tree.Flush()
		return DatabaseRoot{CommitSequence: tx.Sequence(), CatalogRoot: originalRoot, DocumentCount: 1000}, err
	}); err != nil {
		t.Fatal(err)
	}
	beforeDeletePages := file.Meta().PhysicalPageCount

	var reducedRoot uint64
	if err := file.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		tree, err := tx.OpenTree(originalRoot, TreePrimary)
		if err != nil {
			return DatabaseRoot{}, err
		}
		for index := 0; index < 990; index++ {
			removed, err := tree.Delete([]byte(fmt.Sprintf("%04d", index)))
			if err != nil || !removed {
				return DatabaseRoot{}, fmt.Errorf("delete %d removed=%t: %w", index, removed, err)
			}
		}
		removed, err := tree.Delete([]byte("missing"))
		if err != nil || removed {
			return DatabaseRoot{}, fmt.Errorf("missing delete removed=%t: %w", removed, err)
		}
		reducedRoot, err = tree.Flush()
		return DatabaseRoot{CommitSequence: tx.Sequence(), CatalogRoot: reducedRoot, DocumentCount: 10}, err
	}); err != nil {
		t.Fatal(err)
	}
	if addedPages := file.Meta().PhysicalPageCount - beforeDeletePages; addedPages > 8 {
		t.Fatalf("delete wrote %d pages; expected merged reduced tree, not rebuild", addedPages)
	}
	remaining, err := file.TreeScan(reducedRoot, TreePrimary, nil, nil, 0)
	if err != nil || len(remaining) != 10 || string(remaining[0].Key) != "0990" || string(remaining[9].Key) != "0999" {
		t.Fatalf("remaining len=%d first=%q last=%q err=%v", len(remaining), remaining[0].Key, remaining[len(remaining)-1].Key, err)
	}
	// The previous root remains a valid snapshot after path-copy deletion.
	if _, ok, err := file.TreeGet(originalRoot, TreePrimary, []byte("0001")); err != nil || !ok {
		t.Fatalf("old snapshot lookup ok=%t err=%v", ok, err)
	}
}

func TestCopyOnWriteTreeSplitsSkewedLeafByEncodedBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "byte-balanced-split.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	rollbackErr := errors.New("rollback skewed split")
	if err := file.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		tree, err := tx.OpenTree(0, TreePrimary)
		if err != nil {
			return DatabaseRoot{}, err
		}
		for index := 0; index < 128; index++ {
			if err := tree.Put([]byte(fmt.Sprintf("key-%04d", index)), bytes.Repeat([]byte{byte(index)}, 32)); err != nil {
				return DatabaseRoot{}, err
			}
		}
		if err := tree.Put([]byte("key-0000"), bytes.Repeat([]byte{0x5a}, 15_000)); err != nil {
			return DatabaseRoot{}, err
		}
		return DatabaseRoot{}, rollbackErr
	}); !errors.Is(err, rollbackErr) {
		t.Fatalf("rollback err=%v", err)
	}
	if stats := file.StorageStats(); stats.TreeSplits != 0 || stats.TreeMerges != 0 {
		t.Fatalf("rolled-back structural stats=%+v", stats)
	}
	var root uint64
	if err := file.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		tree, err := tx.OpenTree(0, TreePrimary)
		if err != nil {
			return DatabaseRoot{}, err
		}
		for index := 0; index < 128; index++ {
			if err := tree.Put([]byte(fmt.Sprintf("key-%04d", index)), bytes.Repeat([]byte{byte(index)}, 32)); err != nil {
				return DatabaseRoot{}, err
			}
		}
		// Updating one edge entry makes the leaf much larger than one page.
		// A count-median split puts the 15 KiB value together with 63 small
		// entries and fails, while a byte-balanced boundary isolates it.
		if err := tree.Put([]byte("key-0000"), bytes.Repeat([]byte{0x5a}, 15_000)); err != nil {
			return DatabaseRoot{}, err
		}
		root, err = tree.Flush()
		return DatabaseRoot{CommitSequence: tx.Sequence(), CatalogRoot: root, DocumentCount: 128}, err
	}); err != nil {
		t.Fatal(err)
	}
	if stats := file.StorageStats(); stats.TreeSplits != 1 || stats.TreeMerges != 0 {
		t.Fatalf("skewed split stats=%+v", stats)
	}
	values, err := file.TreeScan(root, TreePrimary, nil, nil, 0)
	if err != nil || len(values) != 128 {
		t.Fatalf("values=%d err=%v", len(values), err)
	}
	if len(values[0].Value) != 15_000 || string(values[127].Key) != "key-0127" {
		t.Fatalf("first=%d last=%q", len(values[0].Value), values[127].Key)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	value, exists, err := reopened.TreeGet(root, TreePrimary, []byte("key-0000"))
	if err != nil || !exists || len(value) != 15_000 {
		t.Fatalf("reopened value=%d exists=%t err=%v", len(value), exists, err)
	}
}

func TestByteBalancedBranchSplitAccountsForPromotedKey(t *testing.T) {
	keys := make([][]byte, 0, 24)
	for prefix := byte('a'); prefix <= 'd'; prefix++ {
		keys = append(keys, append([]byte{prefix}, bytes.Repeat([]byte{'x'}, 3_999)...))
	}
	for index := 0; index < 20; index++ {
		keys = append(keys, []byte(fmt.Sprintf("e-%02d", index)))
	}
	children := make([]*nodeRef, len(keys)+1)
	for index := range children {
		children[index] = &nodeRef{count: 1}
	}
	node := &treeNode{keys: keys, children: children, count: uint64(len(children))}
	if _, err := treeNodeEncodedSize(node); !errors.Is(err, ErrNodeFull) {
		t.Fatalf("oversized branch err=%v", err)
	}
	promoted, ok := byteBalancedBranchSplit(node)
	if !ok {
		t.Fatal("no byte-balanced branch split found")
	}
	left := &treeNode{keys: node.keys[:promoted], children: node.children[:promoted+1]}
	right := &treeNode{keys: node.keys[promoted+1:], children: node.children[promoted+1:]}
	leftSize, leftErr := treeNodeEncodedSize(left)
	rightSize, rightErr := treeNodeEncodedSize(right)
	if leftErr != nil || rightErr != nil || leftSize > PageSize-PageHeaderSize || rightSize > PageSize-PageHeaderSize {
		t.Fatalf("promoted=%d left=%d/%v right=%d/%v", promoted, leftSize, leftErr, rightSize, rightErr)
	}
	countMiddle := len(node.keys) / 2
	if _, err := treeNodeEncodedSize(&treeNode{keys: node.keys[:countMiddle], children: node.children[:countMiddle+1]}); !errors.Is(err, ErrNodeFull) {
		t.Fatalf("count-balanced left unexpectedly fit: %v", err)
	}
}

func TestDeleteResplitsBranchWhenMinimumSeparatorGrows(t *testing.T) {
	longMinimum := append([]byte{'b'}, bytes.Repeat([]byte{'z'}, 3_999)...)
	keys := make([][]byte, 0, 500)
	keys = append(keys, []byte("b"))
	for index := 0; index < 499; index++ {
		keys = append(keys, []byte(fmt.Sprintf("c-%04d", index)))
	}
	children := make([]*nodeRef, len(keys)+1)
	children[0] = testLeafRef([]byte("a"))
	children[1] = &nodeRef{count: 2, node: &treeNode{
		leaf: true, keys: [][]byte{[]byte("b"), longMinimum}, values: [][]byte{nil, nil}, count: 2,
	}}
	for index := 2; index < len(children); index++ {
		children[index] = testLeafRef(keys[index-1])
	}
	rootNode := &treeNode{keys: keys, children: children, count: childCount(children)}
	if _, err := treeNodeEncodedSize(rootNode); err != nil {
		t.Fatalf("initial branch does not fit: %v", err)
	}
	tree := &MutableTree{root: &nodeRef{node: rootNode, count: rootNode.count}}
	removed, err := tree.Delete([]byte("b"))
	if err != nil || !removed {
		t.Fatalf("delete removed=%t err=%v", removed, err)
	}
	if _, exists, err := tree.Get([]byte("b")); err != nil || exists {
		t.Fatalf("deleted key exists=%t err=%v", exists, err)
	}
	if _, exists, err := tree.Get(longMinimum); err != nil || !exists {
		t.Fatalf("long minimum exists=%t err=%v", exists, err)
	}
	values, err := tree.Scan(nil, nil, 0)
	if err != nil || len(values) != 501 || !bytes.Equal(values[1].Key, longMinimum) {
		t.Fatalf("values=%d err=%v", len(values), err)
	}
	assertInMemoryTreeFits(t, tree, tree.root)
}

func testLeafRef(keys ...[]byte) *nodeRef {
	node := &treeNode{leaf: true, keys: cloneByteMatrix(keys), values: make([][]byte, len(keys)), count: uint64(len(keys))}
	return &nodeRef{node: node, count: node.count}
}

func assertInMemoryTreeFits(t *testing.T, tree *MutableTree, ref *nodeRef) {
	t.Helper()
	node, err := tree.load(ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := treeNodeEncodedSize(node); err != nil {
		t.Fatalf("node does not fit after rebalance: %v", err)
	}
	if node.leaf {
		return
	}
	for _, child := range node.children {
		assertInMemoryTreeFits(t, tree, child)
	}
}

func TestCopyOnWriteTreeRandomizedAgainstMap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "random.meld2")
	file, _, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	random := rand.New(rand.NewSource(42))
	model := map[string][]byte{}
	rootPage := uint64(0)
	for generation := 0; generation < 8; generation++ {
		previousRoot := rootPage
		previousModel := cloneStringBytesMap(model)
		err := file.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
			tree, err := tx.OpenTree(rootPage, TreeCommitLog)
			if err != nil {
				return DatabaseRoot{}, err
			}
			for operation := 0; operation < 750; operation++ {
				key := fmt.Sprintf("key-%04d", random.Intn(3000))
				if random.Intn(4) == 0 {
					removed, err := tree.Delete([]byte(key))
					if err != nil || removed != (model[key] != nil) {
						return DatabaseRoot{}, fmt.Errorf("delete %q removed=%t: %w", key, removed, err)
					}
					delete(model, key)
					continue
				}
				value := make([]byte, 1+random.Intn(180))
				_, _ = random.Read(value)
				if err := tree.Put([]byte(key), value); err != nil {
					return DatabaseRoot{}, err
				}
				model[key] = append([]byte(nil), value...)
			}
			rootPage, err = tree.Flush()
			return DatabaseRoot{CommitSequence: tx.Sequence(), CommitLogRoot: rootPage, DocumentCount: uint64(len(model))}, err
		})
		if err != nil {
			t.Fatalf("generation %d: %v", generation, err)
		}
		assertTreeMatchesMap(t, file, rootPage, TreeCommitLog, model)
		if previousRoot != 0 {
			assertTreeMatchesMap(t, file, previousRoot, TreeCommitLog, previousModel)
		}
	}
}

func assertTreeMatchesMap(t *testing.T, file *File, root uint64, kind TreeKind, model map[string][]byte) {
	t.Helper()
	values, err := file.TreeScan(root, kind, nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(model))
	for key := range model {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(values) != len(keys) {
		t.Fatalf("tree values=%d model=%d", len(values), len(keys))
	}
	for index, key := range keys {
		if string(values[index].Key) != key || !bytes.Equal(values[index].Value, model[key]) {
			t.Fatalf("entry %d key=%q want=%q value match=%t", index, values[index].Key, key, bytes.Equal(values[index].Value, model[key]))
		}
	}
}

func cloneStringBytesMap(source map[string][]byte) map[string][]byte {
	result := make(map[string][]byte, len(source))
	for key, value := range source {
		result[key] = append([]byte(nil), value...)
	}
	return result
}
