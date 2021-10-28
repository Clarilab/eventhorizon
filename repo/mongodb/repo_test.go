// Copyright (c) 2015 - The Event Horizon authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mongodb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/mocks"
	"github.com/looplab/eventhorizon/repo"
	"github.com/looplab/eventhorizon/uuid"
)

func TestReadRepoCustomPrefixIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Use MongoDB in Docker with fallback to localhost.
	addr := os.Getenv("MONGODB_ADDR")
	if addr == "" {
		addr = "localhost:27017"
	}
	url := "mongodb://" + addr

	// Get a random DB name.
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	db := "test-" + hex.EncodeToString(b)
	t.Log("using DB:", db)

	prefixes := []string{"fooPrefix", "barPrefix"}
	r, err := NewRepo(url, db, "mocks.Model", WithCustomCollectionPrefix(true, prefixes))
	if err != nil {
		t.Error("there should be no error:", err)
	}
	if r == nil {
		t.Error("there should be a repository")
	}
	defer r.Close(context.Background())

	r.SetEntityFactory(func() eh.Entity {
		return &mocks.Model{}
	})
	if r.InnerRepo(context.Background()) != nil {
		t.Error("the inner repo should be nil")
	}

	for _, prefix := range prefixes {
		repo.AcceptanceTest(t, r, eh.NewContextWithNameSpace(context.Background(), prefix))
		extraRepoTests(t, r, eh.NewContextWithNameSpace(context.Background(), prefix))
	}
}

func TestReadRepoIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Use MongoDB in Docker with fallback to localhost.
	addr := os.Getenv("MONGODB_ADDR")
	if addr == "" {
		addr = "localhost:27017"
	}
	url := "mongodb://" + addr

	// Get a random DB name.
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	db := "test-" + hex.EncodeToString(b)
	t.Log("using DB:", db)

	r, err := NewRepo(url, db, "mocks.Model")
	if err != nil {
		t.Error("there should be no error:", err)
	}
	if r == nil {
		t.Error("there should be a repository")
	}
	defer r.Close(context.Background())

	r.SetEntityFactory(func() eh.Entity {
		return &mocks.Model{}
	})
	if r.InnerRepo(context.Background()) != nil {
		t.Error("the inner repo should be nil")
	}

	repo.AcceptanceTest(t, r, context.Background())
	extraRepoTests(t, r, context.Background())
}

func extraRepoTests(t *testing.T, r *Repo, ctx context.Context) {
	// Insert a custom item.
	modelCustom := &mocks.Model{
		ID:        uuid.New(),
		Content:   "modelCustom",
		CreatedAt: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
	}
	if err := r.Save(ctx, modelCustom); err != nil {
		t.Error("there should be no error:", err)
	}

	// FindCustom by content.
	result, err := r.FindCustom(ctx, func(ctx context.Context, c *mongo.Collection) (*mongo.Cursor, error) {
		return c.Find(ctx, bson.M{"content": "modelCustom"})
	})
	if len(result) != 1 {
		t.Error("there should be one item:", len(result))
	}
	if !reflect.DeepEqual(result[0], modelCustom) {
		t.Error("the item should be correct:", modelCustom)
	}

	// FindCustom with no query.
	result, err = r.FindCustom(ctx, func(ctx context.Context, c *mongo.Collection) (*mongo.Cursor, error) {
		return nil, nil
	})
	var repoErr eh.RepoError
	if !errors.As(err, &repoErr) || !errors.Is(err, ErrInvalidQuery) {
		t.Error("there should be a invalid query error:", err)
	}

	var count int64
	// FindCustom with query execution in the callback.
	_, err = r.FindCustom(ctx, func(ctx context.Context, c *mongo.Collection) (*mongo.Cursor, error) {
		if count, err = c.CountDocuments(ctx, bson.M{}); err != nil {
			t.Error("there should be no error:", err)
		}

		// Be sure to return nil to not execute the query again in FindCustom.
		return nil, nil
	})
	if !errors.As(err, &repoErr) || !errors.Is(err, ErrInvalidQuery) {
		t.Error("there should be a invalid query error:", err)
	}
	if count != 2 {
		t.Error("the count should be correct:", count)
	}

	modelCustom2 := &mocks.Model{
		ID:      uuid.New(),
		Content: "modelCustom2",
	}
	if err := r.Collection(ctx, func(ctx context.Context, c *mongo.Collection) error {
		_, err := c.InsertOne(ctx, modelCustom2)
		return err
	}); err != nil {
		t.Error("there should be no error:", err)
	}
	model, err := r.Find(ctx, modelCustom2.ID)
	if err != nil {
		t.Error("there should be no error:", err)
	}
	if !reflect.DeepEqual(model, modelCustom2) {
		t.Error("the item should be correct:", model)
	}

	// FindCustomIter by content.
	iter, err := r.FindCustomIter(ctx, func(ctx context.Context, c *mongo.Collection) (*mongo.Cursor, error) {
		return c.Find(ctx, bson.M{"content": "modelCustom"})
	})
	if err != nil {
		t.Error("there should be no error:", err)
	}

	if iter.Next(ctx) != true {
		t.Error("the iterator should have results")
	}
	if !reflect.DeepEqual(iter.Value(), modelCustom) {
		t.Error("the item should be correct:", modelCustom)
	}
	if iter.Next(ctx) == true {
		t.Error("the iterator should have no results")
	}
	err = iter.Close(ctx)
	if err != nil {
		t.Error("there should be no error:", err)
	}
}

func TestIntoRepo(t *testing.T) {
	if r := IntoRepo(context.Background(), nil); r != nil {
		t.Error("the repository should be nil:", r)
	}

	other := &mocks.Repo{}
	if r := IntoRepo(context.Background(), other); r != nil {
		t.Error("the repository should be correct:", r)
	}

	// Use MongoDB in Docker with fallback to localhost.
	addr := os.Getenv("MONGODB_ADDR")
	if addr == "" {
		addr = "localhost:27017"
	}
	url := "mongodb://" + addr

	// Get a random DB name.
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	db := "test-" + hex.EncodeToString(b)
	t.Log("using DB:", db)

	inner, err := NewRepo(url, db, "mocks.Model")
	if err != nil {
		t.Error("there should be no error:", err)
	}
	defer inner.Close(context.Background())

	outer := &mocks.Repo{ParentRepo: inner}
	if r := IntoRepo(context.Background(), outer); r != inner {
		t.Error("the repository should be correct:", r)
	}
}
