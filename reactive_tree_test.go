package meldbase

import (
	"fmt"
	"hash/maphash"
	"math/rand"
	"testing"
)

func TestPersistentReactiveTreesPreserveOldRootsAndOrdering(t *testing.T) {
	seed := maphash.MakeSeed()
	random := rand.New(rand.NewSource(42))
	limit := 37
	query, err := CompileQuery(Filter{}, QueryOptions{
		Sort: []SortField{{Path: "score", Direction: 1}, {Path: "name", Direction: -1}}, Skip: 11, Limit: &limit,
	})
	if err != nil {
		t.Fatal(err)
	}
	var byID *reactiveIDNode
	var ordered *reactiveOrderNode
	documents := make([]Document, 2_000)
	ids := make([]DocumentID, len(documents))
	for index := range documents {
		ids[index] = deterministicDocumentID(index)
		documents[index] = Document{
			"_id": ID(ids[index]), "score": Int(int64(random.Intn(41))), "name": String(fmt.Sprintf("name-%03d", random.Intn(127))),
		}
		member := reactiveMember{document: documents[index], position: uint64(index + 1)}
		byID = reactiveIDPut(byID, seed, ids[index], member)
		ordered = reactiveOrderPut(ordered, seed, query, ids[index], member)
	}
	assertReactiveTreesValid(t, query, byID, ordered, len(documents))
	if actual, expected := materializeReactiveOrder(ordered, query.skip, query.limit), query.Execute(documents); !documentSlicesEqual(actual, expected) {
		t.Fatal("initial persistent ordering differs from QuerySpec")
	}
	oldByID, oldOrdered := byID, ordered

	for operation := 0; operation < 1_000; operation++ {
		index := random.Intn(len(ids))
		id := ids[index]
		member, exists := reactiveIDGet(byID, id)
		if !exists {
			continue
		}
		byID = reactiveIDDelete(byID, id)
		ordered = reactiveOrderDelete(ordered, query, id, member)
		if operation%3 != 0 {
			document := member.document.Clone()
			document["score"] = Int(int64(random.Intn(41)))
			document["name"] = String(fmt.Sprintf("updated-%03d", operation))
			member.document = document
			byID = reactiveIDPut(byID, seed, id, member)
			ordered = reactiveOrderPut(ordered, seed, query, id, member)
		}
	}
	assertReactiveTreesValid(t, query, byID, ordered, countReactiveIDs(byID))
	// Immutable roots retained before all mutations still contain the complete
	// original view and yield the original sorted window.
	if countReactiveIDs(oldByID) != len(documents) || reactiveOrderSize(oldOrdered) != uint64(len(documents)) {
		t.Fatal("old roots were mutated by path-copy updates")
	}
	if actual, expected := materializeReactiveOrder(oldOrdered, query.skip, query.limit), query.Execute(documents); !documentSlicesEqual(actual, expected) {
		t.Fatal("old ordered root changed")
	}
}

func assertReactiveTreesValid(t *testing.T, query QuerySpec, byID *reactiveIDNode, ordered *reactiveOrderNode, expected int) {
	t.Helper()
	if countReactiveIDs(byID) != expected || reactiveOrderSize(ordered) != uint64(expected) {
		t.Fatalf("tree counts id=%d ordered=%d expected=%d", countReactiveIDs(byID), reactiveOrderSize(ordered), expected)
	}
	validateReactiveIDNode(t, byID, nil, nil)
	validateReactiveOrderNode(t, query, ordered, nil, nil)
}

func validateReactiveIDNode(t *testing.T, node *reactiveIDNode, lower, upper *DocumentID) {
	t.Helper()
	if node == nil {
		return
	}
	if (lower != nil && compareDocumentIDs(node.id, *lower) <= 0) || (upper != nil && compareDocumentIDs(node.id, *upper) >= 0) {
		t.Fatal("ID tree ordering violated")
	}
	for _, child := range []*reactiveIDNode{node.left, node.right} {
		if child != nil && higherReactivePriority(child.priority, child.id, node.priority, node.id) {
			t.Fatal("ID tree heap priority violated")
		}
	}
	validateReactiveIDNode(t, node.left, lower, &node.id)
	validateReactiveIDNode(t, node.right, &node.id, upper)
}

func validateReactiveOrderNode(t *testing.T, query QuerySpec, node *reactiveOrderNode, lower, upper *reactiveOrderNode) uint64 {
	t.Helper()
	if node == nil {
		return 0
	}
	if (lower != nil && compareReactiveOrder(query, node.id, node.member, lower.id, lower.member) <= 0) ||
		(upper != nil && compareReactiveOrder(query, node.id, node.member, upper.id, upper.member) >= 0) {
		t.Fatal("ordered tree comparison violated")
	}
	for _, child := range []*reactiveOrderNode{node.left, node.right} {
		if child != nil && higherReactivePriority(child.priority, child.id, node.priority, node.id) {
			t.Fatal("ordered tree heap priority violated")
		}
	}
	size := 1 + validateReactiveOrderNode(t, query, node.left, lower, node) + validateReactiveOrderNode(t, query, node.right, node, upper)
	if node.size != size {
		t.Fatalf("ordered tree size=%d expected=%d", node.size, size)
	}
	return size
}

func countReactiveIDs(node *reactiveIDNode) int {
	if node == nil {
		return 0
	}
	return 1 + countReactiveIDs(node.left) + countReactiveIDs(node.right)
}

func deterministicDocumentID(index int) DocumentID {
	index++
	return DocumentID{12: byte(index >> 24), 13: byte(index >> 16), 14: byte(index >> 8), 15: byte(index)}
}
