package v2

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
)

func TestSortedTreeBuilderBuildsMultiLevelTree(t *testing.T) {
	file, _, err := Open(filepath.Join(t.TempDir(), "bulk.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	const entries = 12_000
	var root uint64
	if err := file.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		builder, err := tx.NewSortedTreeBuilder(TreeSecondary)
		if err != nil {
			return DatabaseRoot{}, err
		}
		for index := 0; index < entries; index++ {
			key := []byte(fmt.Sprintf("key-%08d", index))
			value := bytes.Repeat([]byte{byte(index)}, 90+(index%37))
			if err := builder.Add(key, value); err != nil {
				return DatabaseRoot{}, err
			}
		}
		root, err = builder.Finish()
		if err != nil {
			return DatabaseRoot{}, err
		}
		tree, err := tx.OpenTree(root, TreeSecondary)
		if err != nil {
			return DatabaseRoot{}, err
		}
		values, err := tree.Scan([]byte("key-00001100"), []byte("key-00001110"), 0)
		if err != nil || len(values) != 10 {
			t.Fatalf("range values=%d err=%v", len(values), err)
		}
		if tree.root.count != entries {
			t.Fatalf("root count=%d", tree.root.count)
		}
		return DatabaseRoot{CommitSequence: tx.Sequence()}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if root < 2 {
		t.Fatalf("root=%d", root)
	}
}

func TestSortedTreeBuilderRejectsNonIncreasingKeysAndSupportsEmpty(t *testing.T) {
	file, _, err := Open(filepath.Join(t.TempDir(), "bulk-empty.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := file.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		builder, err := tx.NewSortedTreeBuilder(TreeSecondary)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if err := builder.Add([]byte("b"), []byte("value")); err != nil {
			return DatabaseRoot{}, err
		}
		if err := builder.Add([]byte("b"), []byte("duplicate")); err == nil {
			t.Fatal("duplicate key accepted")
		}

		empty, err := tx.NewSortedTreeBuilder(TreeSecondary)
		if err != nil {
			return DatabaseRoot{}, err
		}
		root, err := empty.Finish()
		if err != nil {
			return DatabaseRoot{}, err
		}
		tree, err := tx.OpenTree(root, TreeSecondary)
		if err != nil {
			return DatabaseRoot{}, err
		}
		values, err := tree.Scan(nil, nil, 0)
		if err != nil || len(values) != 0 {
			t.Fatalf("empty values=%v err=%v", values, err)
		}
		return DatabaseRoot{CommitSequence: tx.Sequence()}, nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSortedTreeBuilderBuildsDeepTreeWithLargeSeparators(t *testing.T) {
	file, _, err := Open(filepath.Join(t.TempDir(), "bulk-deep.meld2"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	const entries = 400
	if err := file.Update(func(tx *WriteTxn) (DatabaseRoot, error) {
		builder, err := tx.NewSortedTreeBuilder(TreeSecondary)
		if err != nil {
			return DatabaseRoot{}, err
		}
		prefix := bytes.Repeat([]byte{'k'}, 1800)
		for index := 0; index < entries; index++ {
			key := append(append([]byte(nil), prefix...), []byte(fmt.Sprintf("-%08d", index))...)
			if err := builder.Add(key, []byte{byte(index)}); err != nil {
				return DatabaseRoot{}, err
			}
		}
		root, err := builder.Finish()
		if err != nil {
			return DatabaseRoot{}, err
		}
		tree, err := tx.OpenTree(root, TreeSecondary)
		if err != nil {
			return DatabaseRoot{}, err
		}
		rootNode, err := tree.load(tree.root)
		if err != nil {
			return DatabaseRoot{}, err
		}
		if rootNode.leaf || len(rootNode.children) < 2 {
			t.Fatalf("root=%+v", rootNode)
		}
		firstChild, err := tree.load(rootNode.children[0])
		if err != nil {
			return DatabaseRoot{}, err
		}
		if firstChild.leaf {
			t.Fatal("large-key bulk tree did not reach three levels")
		}
		values, err := tree.Scan(nil, nil, 0)
		if err != nil || len(values) != entries {
			t.Fatalf("values=%d err=%v", len(values), err)
		}
		return DatabaseRoot{CommitSequence: tx.Sequence()}, nil
	}); err != nil {
		t.Fatal(err)
	}
}
