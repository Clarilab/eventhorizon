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
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mongoOptions "go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	// Register uuid.UUID as BSON type.
	_ "github.com/looplab/eventhorizon/codec/bson"

	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/uuid"
)

// EventStore implements an eventhorizon.EventStore for MongoDB using a single
// collection with one document per aggregate/stream which holds its events
// as values.
type EventStore struct {
	useCustomPrefix bool
	customPrefixes  []string
	client          *mongo.Client
	db              *mongo.Database
	aggregates      *mongo.Collection
	eventHandler    eh.EventHandler
}

// NewEventStore creates a new EventStore with a MongoDB URI: `mongodb://hostname`.
func NewEventStore(uri, db string, options ...Option) (*EventStore, error) {
	opts := mongoOptions.Client().ApplyURI(uri)
	opts.SetWriteConcern(writeconcern.New(writeconcern.WMajority()))
	opts.SetReadConcern(readconcern.Majority())
	opts.SetReadPreference(readpref.Primary())

	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		return nil, fmt.Errorf("could not connect to DB: %w", err)
	}

	return NewEventStoreWithClient(client, db, options...)
}

// NewEventStoreWithClient creates a new EventStore with a client.
func NewEventStoreWithClient(client *mongo.Client, db string, options ...Option) (*EventStore, error) {
	if client == nil {
		return nil, fmt.Errorf("missing DB client")
	}

	database := client.Database(db)

	s := &EventStore{
		client:     client,
		db:         database,
		aggregates: database.Collection("events"),
	}

	for _, option := range options {
		if err := option(s); err != nil {
			return nil, fmt.Errorf("error while applying option: %w", err)
		}
	}

	if err := s.client.Ping(context.Background(), readpref.Primary()); err != nil {
		return nil, fmt.Errorf("could not connect to MongoDB: %w", err)
	}

	ctx := context.Background()

	if err := s.createIndices(ctx); err != nil {
		return nil, err
	}

	return s, nil
}

// Option is an option setter used to configure creation.
type Option func(*EventStore) error

// WithEventHandler adds an event handler that will be called when saving events.
// An example would be to add an event bus to publish events.
func WithEventHandler(h eh.EventHandler) Option {
	return func(s *EventStore) error {
		s.eventHandler = h

		return nil
	}
}

// WithCustomCollectionPrefix enables the use of multiple collections in the same db.
// Collections are being selected on runtime using a context value.
func WithCustomCollectionPrefix(useCustomPrefix bool, prefixes []string) Option {
	return func(s *EventStore) error {
		s.useCustomPrefix = useCustomPrefix
		s.customPrefixes = prefixes
		return nil
	}
}

// Save implements the Save method of the eventhorizon.EventStore interface.
func (s *EventStore) Save(ctx context.Context, events []eh.Event, originalVersion int) error {
	if len(events) == 0 {
		return &eh.EventStoreError{
			Err: eh.ErrMissingEvents,
			Op:  eh.EventStoreOpSave,
		}
	}

	dbEvents := make([]evt, len(events))
	id := events[0].AggregateID()
	at := events[0].AggregateType()

	// Build all event records, with incrementing versions starting from the
	// original aggregate version.
	for i, event := range events {
		// Only accept events belonging to the same aggregate.
		if event.AggregateID() != id {
			return &eh.EventStoreError{
				Err:              eh.ErrMismatchedEventAggregateIDs,
				Op:               eh.EventStoreOpSave,
				AggregateType:    at,
				AggregateID:      id,
				AggregateVersion: originalVersion,
				Events:           events,
			}
		}

		if event.AggregateType() != at {
			return &eh.EventStoreError{
				Err:              eh.ErrMismatchedEventAggregateTypes,
				Op:               eh.EventStoreOpSave,
				AggregateType:    at,
				AggregateID:      id,
				AggregateVersion: originalVersion,
				Events:           events,
			}
		}

		// Only accept events that apply to the correct aggregate version.
		if event.Version() != originalVersion+i+1 {
			return &eh.EventStoreError{
				Err:              eh.ErrIncorrectEventVersion,
				Op:               eh.EventStoreOpSave,
				AggregateType:    at,
				AggregateID:      id,
				AggregateVersion: originalVersion,
				Events:           events,
			}
		}

		// Create the event record for the DB.
		e, err := newEvt(ctx, event)
		if err != nil {
			return &eh.EventStoreError{
				Err:              fmt.Errorf("could not copy event: %w", err),
				Op:               eh.EventStoreOpSave,
				AggregateType:    at,
				AggregateID:      id,
				AggregateVersion: originalVersion,
				Events:           events,
			}
		}

		dbEvents[i] = *e
	}

	// Either insert a new aggregate or append to an existing.
	if originalVersion == 0 {
		aggregate := aggregateRecord{
			AggregateID: id,
			Version:     len(dbEvents),
			Events:      dbEvents,
		}

		eventsCollection := s.aggregates
		if s.useCustomPrefix {
			eventsCollection = s.collEvents(ctx)
		}

		if _, err := eventsCollection.InsertOne(ctx, aggregate); err != nil {
			return &eh.EventStoreError{
				Err:              fmt.Errorf("could not insert: %w", err),
				Op:               eh.EventStoreOpSave,
				AggregateType:    at,
				AggregateID:      id,
				AggregateVersion: originalVersion,
				Events:           events,
			}
		}
	} else {
		// Increment aggregate version on insert of new event record, and
		// only insert if version of aggregate is matching (ie not changed
		// since loading the aggregate).
		eventsCollection := s.aggregates
		if s.useCustomPrefix {
			eventsCollection = s.collEvents(ctx)
		}

		if r, err := eventsCollection.UpdateOne(ctx,
			bson.M{
				"_id":     id,
				"version": originalVersion,
			},
			bson.M{
				"$push": bson.M{"events": bson.M{"$each": dbEvents}},
				"$inc":  bson.M{"version": len(dbEvents)},
			},
		); err != nil {
			return &eh.EventStoreError{
				Err:              fmt.Errorf("could not update: %w", err),
				Op:               eh.EventStoreOpSave,
				AggregateType:    at,
				AggregateID:      id,
				AggregateVersion: originalVersion,
				Events:           events,
			}
		} else if r.MatchedCount == 0 {
			return &eh.EventStoreError{
				Err:              eh.ErrEventConflictFromOtherSave,
				Op:               eh.EventStoreOpSave,
				AggregateType:    at,
				AggregateID:      id,
				AggregateVersion: originalVersion,
				Events:           events,
			}
		}
	}

	// Let the optional event handler handle the events.
	if s.eventHandler != nil {
		for _, e := range events {
			if err := s.eventHandler.HandleEvent(ctx, e); err != nil {
				return &eh.EventHandlerError{
					Err:   err,
					Event: e,
				}
			}
		}
	}

	return nil
}

// Load implements the Load method of the eventhorizon.EventStore interface.
func (s *EventStore) Load(ctx context.Context, id uuid.UUID) ([]eh.Event, error) {
	var aggregate aggregateRecord
	eventsCollection := s.aggregates
	if s.useCustomPrefix {
		eventsCollection = s.collEvents(ctx)
	}

	if err := eventsCollection.FindOne(ctx, bson.M{"_id": id}).Decode(&aggregate); err != nil {
		// Translate to our own not found error.
		if err == mongo.ErrNoDocuments {
			err = eh.ErrAggregateNotFound
		}

		return nil, &eh.EventStoreError{
			Err:         err,
			Op:          eh.EventStoreOpLoad,
			AggregateID: id,
		}
	}

	events := make([]eh.Event, len(aggregate.Events))

	for i, e := range aggregate.Events {
		// Create an event of the correct type and decode from raw BSON.
		if len(e.RawData) > 0 {
			var err error
			if e.data, err = eh.CreateEventData(e.EventType); err != nil {
				return nil, &eh.EventStoreError{
					Err:              fmt.Errorf("could not create event data: %w", err),
					Op:               eh.EventStoreOpLoad,
					AggregateType:    e.AggregateType,
					AggregateID:      id,
					AggregateVersion: e.Version,
					Events:           events,
				}
			}

			if err := bson.Unmarshal(e.RawData, e.data); err != nil {
				return nil, &eh.EventStoreError{
					Err:              fmt.Errorf("could not unmarshal event data: %w", err),
					Op:               eh.EventStoreOpLoad,
					AggregateType:    e.AggregateType,
					AggregateID:      id,
					AggregateVersion: e.Version,
					Events:           events,
				}
			}

			e.RawData = nil
		}

		event := eh.NewEvent(
			e.EventType,
			e.data,
			e.Timestamp,
			eh.ForAggregate(
				e.AggregateType,
				e.AggregateID,
				e.Version,
			),
			eh.WithMetadata(e.Metadata),
		)

		events[i] = event
	}

	return events, nil
}

// Close implements the Close method of the eventhorizon.EventStore interface.
func (s *EventStore) Close() error {
	return s.client.Disconnect(context.Background())
}

// aggregateRecord is the Database representation of an aggregate.
type aggregateRecord struct {
	AggregateID uuid.UUID `bson:"_id"`
	Version     int       `bson:"version"`
	Events      []evt     `bson:"events"`
	// Type        string        `bson:"type"`
	// Snapshot    bson.Raw      `bson:"snapshot"`
}

// evt is the internal event record for the MongoDB event store used
// to save and load events from the DB.
type evt struct {
	EventType     eh.EventType           `bson:"event_type"`
	RawData       bson.Raw               `bson:"data,omitempty"`
	data          eh.EventData           `bson:"-"`
	Timestamp     time.Time              `bson:"timestamp"`
	AggregateType eh.AggregateType       `bson:"aggregate_type"`
	AggregateID   uuid.UUID              `bson:"_id"`
	Version       int                    `bson:"version"`
	Metadata      map[string]interface{} `bson:"metadata"`
}

// newEvt returns a new evt for an event.
func newEvt(ctx context.Context, event eh.Event) (*evt, error) {
	e := &evt{
		EventType:     event.EventType(),
		Timestamp:     event.Timestamp(),
		AggregateType: event.AggregateType(),
		AggregateID:   event.AggregateID(),
		Version:       event.Version(),
		Metadata:      event.Metadata(),
	}

	// Marshal event data if there is any.
	if event.Data() != nil {
		var err error

		e.RawData, err = bson.Marshal(event.Data())
		if err != nil {
			return nil, fmt.Errorf("could not marshal event data: %w", err)
		}
	}

	return e, nil
}

// collEvents returns a collection. If a custom prefix can be fetched from context that one is used. Otherwise returns default_events collection.
func (s *EventStore) collEvents(ctx context.Context) *mongo.Collection {
	ns := eh.NamespaceFromContext(ctx)

	return s.db.Collection(ns + "_events")
}

// createIndices creates indices.
func (s *EventStore) createIndices(ctx context.Context) error {
	if !s.useCustomPrefix {
		if _, err := s.aggregates.Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys: bson.M{"_id": 1},
		}); err != nil {
			return fmt.Errorf("could not ensure events index: %w", err)
		}

		return nil
	}

	for i := range s.customPrefixes {
		prefix := s.customPrefixes[i]
		collection := s.db.Collection(prefix + "_events")

		if _, err := collection.Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys: bson.M{"_id": 1},
		}); err != nil {
			return fmt.Errorf("could not ensure events index: %w", err)
		}
	}

	return nil
}
