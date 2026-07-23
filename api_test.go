package meldbase_test

import (
	"context"
	"testing"

	"github.com/crapthings/meldbase"
)

func TestPublicDatabaseAPI(t *testing.T) {
	database := meldbase.New()
	t.Cleanup(func() { _ = database.Close() })

	collection := database.Collection("items")
	if _, err := collection.InsertOne(context.Background(), meldbase.Document{"name": meldbase.String("root-api")}); err != nil {
		t.Fatalf("insert through root API: %v", err)
	}
	result, err := collection.FindOne(context.Background(), meldbase.Filter{})
	if err != nil {
		t.Fatalf("find through root API: %v", err)
	}
	if !result["name"].Equal(meldbase.String("root-api")) {
		t.Fatalf("root API document = %#v", result)
	}
}
