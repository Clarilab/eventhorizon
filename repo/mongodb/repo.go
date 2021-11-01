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
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	mongoOptions "go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	// Register uuid.UUID as BSON type.
	_ "github.com/looplab/eventhorizon/codec/bson"

	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/uuid"
)

// ErrModelNotSet is when an model factory is not set on the Repo.
var ErrModelNotSet = errors.New("model not set")

// Repo implements an MongoDB repository for entities.
type Repo struct {
	useCustomPrefix bool
	customPrefixes  []string
	collSuffix      string
	client          *mongo.Client
	db              *mongo.Database
	entities        *mongo.Collection
	newEntity       func() eh.Entity
}

// NewRepo creates a new Repo.
func NewRepo(uri, db, collection string, options ...Option) (*Repo, error) {
	opts := mongoOptions.Client().ApplyURI(uri)
	opts.SetWriteConcern(writeconcern.New(writeconcern.WMajority()))
	opts.SetReadConcern(readconcern.Majority())
	opts.SetReadPreference(readpref.Primary())
	client, err := mongo.Connect(context.TODO(), opts)

	if err != nil {
		return nil, fmt.Errorf("could not connect to DB: %w", err)
	}

	return NewRepoWithClient(client, db, collection, options...)
}

// NewRepoWithClient creates a new Repo with a client.
func NewRepoWithClient(client *mongo.Client, db, collection string, options ...Option) (*Repo, error) {
	if client == nil {
		return nil, fmt.Errorf("missing DB client")
	}

	database := client.Database(db)

	r := &Repo{
		client:     client,
		db:         database,
		entities:   database.Collection(collection),
		collSuffix: collection,
	}

	for _, option := range options {
		if err := option(r); err != nil {
			return nil, fmt.Errorf("error while applying option: %w", err)
		}
	}

	if err := r.client.Ping(context.Background(), readpref.Primary()); err != nil {
		return nil, fmt.Errorf("could not connect to MongoDB: %w", err)
	}

	return r, nil
}

// Option is an option setter used to configure creation.
type Option func(*Repo) error

// InnerRepo implements the InnerRepo method of the eventhorizon.ReadRepo interface.
func (r *Repo) InnerRepo(ctx context.Context) eh.ReadRepo {
	return nil
}

// IntoRepo tries to convert a eh.ReadRepo into a Repo by recursively looking at
// inner repos. Returns nil if none was found.
func IntoRepo(ctx context.Context, repo eh.ReadRepo) *Repo {
	if repo == nil {
		return nil
	}

	if r, ok := repo.(*Repo); ok {
		return r
	}

	return IntoRepo(ctx, repo.InnerRepo(ctx))
}

// WithCustomCollectionPrefix enables the use of multiple collections in the same db.
// Collections are being selected on runtime using a context value.
func WithCustomCollectionPrefix(useCustomPrefix bool, prefixes []string) Option {
	return func(r *Repo) error {
		r.useCustomPrefix = useCustomPrefix
		r.customPrefixes = prefixes
		return nil
	}
}

// Find implements the Find method of the eventhorizon.ReadRepo interface.
func (r *Repo) Find(ctx context.Context, id uuid.UUID) (eh.Entity, error) {
	if r.newEntity == nil {
		return nil, &eh.RepoError{
			Err:      ErrModelNotSet,
			Op:       eh.RepoOpFind,
			EntityID: id,
		}
	}

	entitiesCollection := r.entities
	if r.useCustomPrefix {
		entitiesCollection = r.collEntities(ctx)
	}

	entity := r.newEntity()
	if err := entitiesCollection.FindOne(ctx, bson.M{"_id": id.String()}).Decode(entity); err != nil {
		if err == mongo.ErrNoDocuments {
			err = eh.ErrEntityNotFound
		}

		return nil, &eh.RepoError{
			Err:      err,
			Op:       eh.RepoOpFind,
			EntityID: id,
		}
	}

	return entity, nil
}

// FindAll implements the FindAll method of the eventhorizon.ReadRepo interface.
func (r *Repo) FindAll(ctx context.Context) ([]eh.Entity, error) {
	if r.newEntity == nil {
		return nil, &eh.RepoError{
			Err: ErrModelNotSet,
			Op:  eh.RepoOpFindAll,
		}
	}

	entitiesCollection := r.entities
	if r.useCustomPrefix {
		entitiesCollection = r.collEntities(ctx)
	}

	cursor, err := entitiesCollection.Find(ctx, bson.M{})
	if err != nil {
		return nil, &eh.RepoError{
			Err: fmt.Errorf("could not find: %w", err),
			Op:  eh.RepoOpFindAll,
		}
	}

	result := []eh.Entity{}

	for cursor.Next(ctx) {
		entity := r.newEntity()
		if err := cursor.Decode(entity); err != nil {
			return nil, &eh.RepoError{
				Err: fmt.Errorf("could not unmarshal: %w", err),
				Op:  eh.RepoOpFindAll,
			}
		}

		result = append(result, entity)
	}

	if err := cursor.Close(ctx); err != nil {
		return nil, &eh.RepoError{
			Err: fmt.Errorf("could not close cursor: %w", err),
			Op:  eh.RepoOpFindAll,
		}
	}

	return result, nil
}

// The iterator is not thread safe.
type iter struct {
	cursor    *mongo.Cursor
	data      eh.Entity
	newEntity func() eh.Entity
	decodeErr error
}

func (i *iter) Next(ctx context.Context) bool {
	if !i.cursor.Next(ctx) {
		return false
	}

	item := i.newEntity()
	i.decodeErr = i.cursor.Decode(item)
	i.data = item

	return true
}

func (i *iter) Value() interface{} {
	return i.data
}

func (i *iter) Close(ctx context.Context) error {
	if err := i.cursor.Close(ctx); err != nil {
		return err
	}

	return i.decodeErr
}

// FindCustomIter returns a mgo cursor you can use to stream results of very large datasets.
func (r *Repo) FindCustomIter(ctx context.Context, f func(context.Context, *mongo.Collection) (*mongo.Cursor, error)) (eh.Iter, error) {
	if r.newEntity == nil {
		return nil, &eh.RepoError{
			Err: ErrModelNotSet,
			Op:  eh.RepoOpFindQuery,
		}
	}

	entitiesCollection := r.entities
	if r.useCustomPrefix {
		entitiesCollection = r.collEntities(ctx)
	}

	cursor, err := f(ctx, entitiesCollection)
	if err != nil {
		return nil, &eh.RepoError{
			Err: fmt.Errorf("could not find: %w", err),
			Op:  eh.RepoOpFindQuery,
		}
	}

	if cursor == nil {
		return nil, &eh.RepoError{
			Err: fmt.Errorf("no cursor"),
			Op:  eh.RepoOpFindQuery,
		}
	}

	return &iter{
		cursor:    cursor,
		newEntity: r.newEntity,
	}, nil
}

// FindCustom uses a callback to specify a custom query for returning models.
// It can also be used to do queries that does not map to the model by executing
// the query in the callback and returning nil to block a second execution of
// the same query in FindCustom. Expect a ErrInvalidQuery if returning a nil
// query from the callback.
func (r *Repo) FindCustom(ctx context.Context, f func(context.Context, *mongo.Collection) (*mongo.Cursor, error)) ([]interface{}, error) {
	if r.newEntity == nil {
		return nil, &eh.RepoError{
			Err: ErrModelNotSet,
			Op:  eh.RepoOpFindQuery,
		}
	}

	entitiesCollection := r.entities
	if r.useCustomPrefix {
		entitiesCollection = r.collEntities(ctx)
	}

	cursor, err := f(ctx, entitiesCollection)
	if err != nil {
		return nil, &eh.RepoError{
			Err: fmt.Errorf("could not find: %w", err),
			Op:  eh.RepoOpFindQuery,
		}
	}

	if cursor == nil {
		return nil, &eh.RepoError{
			Err: fmt.Errorf("no cursor"),
			Op:  eh.RepoOpFindQuery,
		}
	}

	result := []interface{}{}
	entity := r.newEntity()

	for cursor.Next(ctx) {
		if err := cursor.Decode(entity); err != nil {
			return nil, &eh.RepoError{
				Err: fmt.Errorf("could not unmarshal: %w", err),
				Op:  eh.RepoOpFindQuery,
			}
		}

		result = append(result, entity)
		entity = r.newEntity()
	}

	if err := cursor.Close(ctx); err != nil {
		return nil, &eh.RepoError{
			Err: fmt.Errorf("could not close cursor: %w", err),
			Op:  eh.RepoOpFindQuery,
		}
	}

	return result, nil
}

// Save implements the Save method of the eventhorizon.WriteRepo interface.
func (r *Repo) Save(ctx context.Context, entity eh.Entity) error {
	id := entity.EntityID()
	if id == uuid.Nil {
		return &eh.RepoError{
			Err: fmt.Errorf("missing entity ID"),
			Op:  eh.RepoOpSave,
		}
	}

	entitiesCollection := r.entities
	if r.useCustomPrefix {
		entitiesCollection = r.collEntities(ctx)
	}

	if _, err := entitiesCollection.UpdateOne(ctx,
		bson.M{
			"_id": id.String(),
		},
		bson.M{
			"$set": entity,
		},
		options.Update().SetUpsert(true),
	); err != nil {
		return &eh.RepoError{
			Err:      fmt.Errorf("could not save/update: %w", err),
			Op:       eh.RepoOpSave,
			EntityID: id,
		}
	}

	return nil
}

// Remove implements the Remove method of the eventhorizon.WriteRepo interface.
func (r *Repo) Remove(ctx context.Context, id uuid.UUID) error {
	entitiesCollection := r.entities
	if r.useCustomPrefix {
		entitiesCollection = r.collEntities(ctx)
	}

	if r, err := entitiesCollection.DeleteOne(ctx, bson.M{"_id": id.String()}); err != nil {
		return &eh.RepoError{
			Err:      err,
			Op:       eh.RepoOpRemove,
			EntityID: id,
		}
	} else if r.DeletedCount == 0 {
		return &eh.RepoError{
			Err:      eh.ErrEntityNotFound,
			Op:       eh.RepoOpRemove,
			EntityID: id,
		}
	}

	return nil
}

// Collection lets the function do custom actions on the collection.
func (r *Repo) Collection(ctx context.Context, f func(context.Context, *mongo.Collection) error) error {
	entitiesCollection := r.entities
	if r.useCustomPrefix {
		entitiesCollection = r.collEntities(ctx)
	}

	if err := f(ctx, entitiesCollection); err != nil {
		return &eh.RepoError{
			Err: err,
		}
	}

	return nil
}

// CreateIndex creates an index for a field.
func (r *Repo) CreateIndex(ctx context.Context, field string) error {
	if !r.useCustomPrefix {
		index := mongo.IndexModel{Keys: bson.M{field: 1}}
		if _, err := r.entities.Indexes().CreateOne(ctx, index); err != nil {
			return fmt.Errorf("could not create index: %s", err)
		}
		return nil
	}

	for i := range r.customPrefixes {
		prefix := r.customPrefixes[i]
		collection := r.db.Collection(prefix + r.collSuffix)

		index := mongo.IndexModel{Keys: bson.M{field: 1}}
		if _, err := collection.Indexes().CreateOne(ctx, index); err != nil {
			return fmt.Errorf("could not create index: %s", err)
		}
	}

	return nil
}

// collEntities returns a collection. If a custom prefix can be fetched from context that one is used. Otherwise returns default_{collSuffix} collection.
func (r *Repo) collEntities(ctx context.Context) *mongo.Collection {
	ns := eh.NamespaceFromContext(ctx)

	return r.db.Collection(ns + "_" + r.collSuffix)
}

// SetEntityFactory sets a factory function that creates concrete entity types.
func (r *Repo) SetEntityFactory(f func() eh.Entity) {
	r.newEntity = f
}

// Clear clears the read model database.
func (r *Repo) Clear(ctx context.Context) error {
	entitiesCollection := r.entities
	if r.useCustomPrefix {
		entitiesCollection = r.collEntities(ctx)
	}

	if err := entitiesCollection.Drop(ctx); err != nil {
		return &eh.RepoError{
			Err: fmt.Errorf("could not drop collection: %w", err),
			Op:  eh.RepoOpClear,
		}
	}

	return nil
}

// Close implements the Close method of the eventhorizon.WriteRepo interface.
func (r *Repo) Close() error {
	return r.client.Disconnect(context.Background())
}
