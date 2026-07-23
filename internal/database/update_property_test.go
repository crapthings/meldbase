package database

import (
	"context"
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"
)

func TestRandomUpdateSequencesMatchSimpleReferenceModel(t *testing.T) {
	for seed := uint64(1); seed <= 12; seed++ {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			db := New()
			defer db.Close()
			collection := db.Collection("items")
			id, err := collection.InsertOne(context.Background(), Document{"n": Int(0), "items": Array(), "label": String("initial")})
			if err != nil {
				t.Fatal(err)
			}
			random := rand.New(rand.NewPCG(seed, seed^0xda942042e4dd58b5))
			number, label := int64(0), "initial"
			labelExists := true
			items := []int64{}
			for step := 0; step < 300; step++ {
				var update Update
				switch random.Uint64() % 5 {
				case 0:
					delta := int64(random.Uint64()%11) - 5
					update = Update{"$inc": map[string]any{"n": delta}}
					number += delta
				case 1:
					label = fmt.Sprintf("label-%d", random.Uint64()%19)
					labelExists = true
					update = Update{"$set": map[string]any{"label": label}}
				case 2:
					labelExists = false
					update = Update{"$unset": map[string]any{"label": true}}
				case 3:
					value := int64(random.Uint64() % 9)
					items = append(items, value)
					update = Update{"$push": map[string]any{"items": value}}
				case 4:
					value := int64(random.Uint64() % 9)
					kept := items[:0]
					for _, item := range items {
						if item != value {
							kept = append(kept, item)
						}
					}
					items = kept
					update = Update{"$pull": map[string]any{"items": value}}
				}
				if _, err := collection.UpdateOne(context.Background(), Filter{"_id": id}, update); err != nil {
					t.Fatalf("step %d: %v", step, err)
				}
				document, err := collection.FindOne(context.Background(), Filter{"_id": id})
				if err != nil {
					t.Fatal(err)
				}
				actualNumber, ok := document["n"].Int64()
				if !ok || actualNumber != number {
					t.Fatalf("step %d number=%d want=%d", step, actualNumber, number)
				}
				actualLabel, found := document["label"]
				actualLabelText, isString := actualLabel.StringValue()
				if found != labelExists || (found && (!isString || actualLabelText != label)) {
					t.Fatalf("step %d label=%v found=%v want=%q exists=%v", step, actualLabel, found, label, labelExists)
				}
				actualItems, ok := document["items"].ArrayValue()
				if !ok {
					t.Fatalf("step %d items is not array", step)
				}
				got := make([]int64, len(actualItems))
				for index, item := range actualItems {
					got[index], _ = item.Int64()
				}
				if !reflect.DeepEqual(got, items) {
					t.Fatalf("step %d items=%v want=%v", step, got, items)
				}
			}
		})
	}
}
