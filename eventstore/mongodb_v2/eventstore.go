// Copyright (c) 2021 - The Event Horizon authors
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

package mongodb_v2

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

// EventStore is an eventhorizon.EventStore for MongoDB, using one collection
// for all events and another to keep track of all aggregates/streams. It also
// keep tracks of the global position of events, stored as metadata.
type EventStore struct {
	useCustomPrefix bool
	customPrefixes  []string
	client          *mongo.Client
	db              *mongo.Database
	events          *mongo.Collection
	streams         *mongo.Collection
	eventHandler    eh.EventHandler
}

// NewEventStore creates a new EventStore with a MongoDB URI: `mongodb://hostname`.
func NewEventStore(uri, dbName string, options ...Option) (*EventStore, error) {
	opts := mongoOptions.Client().ApplyURI(uri)
	opts.SetWriteConcern(writeconcern.New(writeconcern.WMajority()))
	opts.SetReadConcern(readconcern.Majority())
	opts.SetReadPreference(readpref.Primary())
	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		return nil, fmt.Errorf("could not connect to DB: %w", err)
	}

	return NewEventStoreWithClient(client, dbName, options...)
}

// NewEventStoreWithClient creates a new EventStore with a client.
func NewEventStoreWithClient(client *mongo.Client, dbName string, options ...Option) (*EventStore, error) {
	if client == nil {
		return nil, fmt.Errorf("missing DB client")
	}

	db := client.Database(dbName)

	s := &EventStore{
		client:  client,
		db:      db,
		events:  db.Collection("events"),
		streams: db.Collection("streams"),
	}

	for _, option := range options {
		if err := option(s); err != nil {
			return nil, fmt.Errorf("error while applying option: %w", err)
		}
	}

	ctx := context.Background()

	err := s.createIndices(ctx)
	if err != nil {
		return nil, err
	}

	err = s.ensureAllStream(ctx)
	if err != nil {
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
func WithCustomCollectionPrefix(customCollections bool, prefixes []string) Option {
	return func(s *EventStore) error {
		s.useCustomPrefix = customCollections
		s.customPrefixes = prefixes
		return nil
	}
}

// Save implements the Save method of the eventhorizon.EventStore interface.
func (s *EventStore) Save(ctx context.Context, events []eh.Event, originalVersion int) error {
	if len(events) == 0 {
		return eh.EventStoreError{
			Err: eh.ErrNoEventsToAppend,
		}
	}

	// Build all event records, with incrementing versions starting from the
	// original aggregate version.
	dbEvents := make([]interface{}, len(events))
	aggregateID := events[0].AggregateID()
	for i, event := range events {
		// Only accept events belonging to the same aggregate.
		if event.AggregateID() != aggregateID {
			return eh.EventStoreError{
				Err: eh.ErrInvalidEvent,
			}
		}

		// Only accept events that apply to the correct aggregate version.
		if event.Version() != originalVersion+i+1 {
			return eh.EventStoreError{
				Err: eh.ErrIncorrectEventVersion,
			}
		}

		// Create the event record for the DB.
		e, err := newEvt(ctx, event)
		if err != nil {
			return err
		}
		dbEvents[i] = e
	}

	sess, err := s.client.StartSession(nil)
	if err != nil {
		return eh.EventStoreError{
			Err:     eh.ErrCouldNotSaveEvents,
			BaseErr: err,
		}
	}
	defer sess.EndSession(ctx)

	if _, err := sess.WithTransaction(ctx, func(txCtx mongo.SessionContext) (interface{}, error) {
		// Fetch and increment global version in the all-stream.
		streams := s.streams
		if s.useCustomPrefix {
			streams = s.collStreams(ctx)
		}

		r := streams.FindOneAndUpdate(txCtx,
			bson.M{"_id": "$all"},
			bson.M{"$inc": bson.M{"position": len(dbEvents)}},
		)
		if r.Err() != nil {
			return nil, fmt.Errorf("could not increment global position: %w", r.Err())
		}

		allStream := struct {
			Position int
		}{}
		if err := r.Decode(&allStream); err != nil {
			return nil, fmt.Errorf("could not decode global position: %w", err)
		}

		// Use the global position as ID for the stored events.
		// This natively prevents duplicate events to be written.
		var strm *stream
		for i, e := range dbEvents {
			event := e.(*evt)
			event.Position = allStream.Position + i + 1
			// Also store the position in the event metadata.
			event.Metadata["position"] = event.Position

			// Use the last event to set the new stream position.
			if i == len(dbEvents)-1 {
				strm = &stream{
					ID:            event.AggregateID,
					Position:      event.Position,
					AggregateType: event.AggregateType,
					Version:       event.Version,
					UpdatedAt:     event.Timestamp,
				}
			}
		}

		// Store events.
		eventsCollection := s.events
		if s.useCustomPrefix {
			eventsCollection = s.collEvents(ctx)
		}

		insert, err := eventsCollection.InsertMany(txCtx, dbEvents)
		if err != nil {
			return nil, fmt.Errorf("could not insert events: %w", err)
		}

		// Check that all inserted events got the requested ID (position),
		// instead of a generated ID by MongoDB.
		for _, e := range dbEvents {
			event := e.(*evt)
			found := false
			for _, id := range insert.InsertedIDs {
				if pos, ok := id.(int32); ok && event.Position == int(pos) {
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("inserted event %s at pos %d not found",
					event.AggregateID, event.Position)
			}
		}

		streamsCollection := s.streams
		if s.useCustomPrefix {
			streamsCollection = s.collEvents(ctx)
		}

		// Update the stream.
		if originalVersion == 0 {
			if _, err := streamsCollection.InsertOne(txCtx, strm); err != nil {
				return nil, fmt.Errorf("could not insert stream: %w", err)
			}
		} else {
			if res := streamsCollection.FindOneAndUpdate(txCtx,
				bson.M{"_id": strm.ID},
				bson.M{
					"$set": bson.M{
						"position":   strm.Position,
						"version":    strm.Version,
						"updated_at": strm.UpdatedAt,
					},
				},
				mongoOptions.FindOneAndUpdate().SetUpsert(true),
			); res.Err() != nil {
				return nil, fmt.Errorf("could not update stream: %w", res.Err())
			}
		}

		return nil, nil
	}); err != nil {
		return eh.EventStoreError{
			Err:     eh.ErrCouldNotSaveEvents,
			BaseErr: err,
		}
	}

	// Let the optional event handler handle the events.
	if s.eventHandler != nil {
		for _, e := range events {
			if err := s.eventHandler.HandleEvent(ctx, e); err != nil {
				return eh.CouldNotHandleEventError{
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
	eventsCollection := s.events
	if s.useCustomPrefix {
		eventsCollection = s.collEvents(ctx)
	}

	cursor, err := eventsCollection.Find(ctx, bson.M{"aggregate_id": id})
	if err != nil {
		return nil, eh.EventStoreError{
			Err: fmt.Errorf("could not find event: %w", err),
		}
	}

	var events []eh.Event
	for cursor.Next(ctx) {
		var e evt
		if err := cursor.Decode(&e); err != nil {
			return nil, eh.EventStoreError{
				Err: fmt.Errorf("could not decode event: %w", err),
			}
		}

		// Create an event of the correct type and decode from raw BSON.
		if len(e.RawData) > 0 {
			var err error
			if e.data, err = eh.CreateEventData(e.EventType); err != nil {
				return nil, eh.EventStoreError{
					Err: fmt.Errorf("could not create event data: %w", err),
				}
			}
			if err := bson.Unmarshal(e.RawData, e.data); err != nil {
				return nil, eh.EventStoreError{
					Err: fmt.Errorf("could not unmarshal event data: %w", err),
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
		events = append(events, event)
	}

	return events, nil
}

// Close closes the database client.
func (s *EventStore) Close(ctx context.Context) error {
	if err := s.client.Disconnect(ctx); err != nil {
		return fmt.Errorf("could not close DB connection: %w", err)
	}
	return nil
}

// stream is a stream of events, often containing the events for an aggregate.
type stream struct {
	ID            uuid.UUID        `bson:"_id"`
	Position      int              `bson:"position"`
	AggregateType eh.AggregateType `bson:"aggregate_type"`
	Version       int              `bson:"version"`
	UpdatedAt     time.Time        `bson:"updated_at"`
}

// evt is the internal event record for the MongoDB event store used
// to save and load events from the DB.
type evt struct {
	Position      int                    `bson:"_id"`
	EventType     eh.EventType           `bson:"event_type"`
	Timestamp     time.Time              `bson:"timestamp"`
	AggregateType eh.AggregateType       `bson:"aggregate_type"`
	AggregateID   uuid.UUID              `bson:"aggregate_id"`
	Version       int                    `bson:"version"`
	RawData       bson.Raw               `bson:"data,omitempty"`
	data          eh.EventData           `bson:"-"`
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

	if e.Metadata == nil {
		e.Metadata = map[string]interface{}{}
	}

	// Marshal event data if there is any.
	if event.Data() != nil {
		var err error
		e.RawData, err = bson.Marshal(event.Data())
		if err != nil {
			return nil, eh.EventStoreError{
				Err: fmt.Errorf("could not marshal event data: %w", err),
			}
		}
	}

	return e, nil
}

// collEvents appends the namespace, if one is set, to the Collection to
// get the name of the Collection to use.
func (s *EventStore) collEvents(ctx context.Context) *mongo.Collection {
	ns := eh.NamespaceFromContext(ctx)

	return s.db.Collection(ns + "_events")
}

// collStreams appends the namespace, if one is set, to the Collection to
// get the name of the Collection to use.
func (s *EventStore) collStreams(ctx context.Context) *mongo.Collection {
	ns := eh.NamespaceFromContext(ctx)

	return s.db.Collection(ns + "_streams")
}

// ensureAllStream ensures that the $all stream exist for all collections
func (s *EventStore) ensureAllStream(ctx context.Context) error {
	if !s.useCustomPrefix {
		// Make sure the $all stream exists.
		if err := s.streams.FindOne(ctx, bson.M{
			"_id": "$all",
		}).Err(); err == mongo.ErrNoDocuments {
			if _, err := s.streams.InsertOne(ctx, bson.M{
				"_id":      "$all",
				"position": 0,
			}); err != nil {
				return fmt.Errorf("could not create the $all stream document: %w", err)
			}
		} else if err != nil {
			return fmt.Errorf("could not find the $all stream document: %w", err)
		}
		return nil
	}

	for i := range s.customPrefixes {
		prefix := s.customPrefixes[i]
		collection := s.db.Collection(prefix + "_streams")

		if err := collection.FindOne(ctx, bson.M{
			"_id": "$all",
		}).Err(); err == mongo.ErrNoDocuments {
			if _, err := collection.InsertOne(ctx, bson.M{
				"_id":      "$all",
				"position": 0,
			}); err != nil {
				return fmt.Errorf("could not create the $all stream document: %w", err)
			}
		} else if err != nil {
			return fmt.Errorf("could not find the $all stream document: %w", err)
		}
	}

	return nil
}

// createIndices creates incides.
func (s *EventStore) createIndices(ctx context.Context) error {
	if !s.useCustomPrefix {
		if _, err := s.events.Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys: bson.M{"aggregate_id": 1},
		}); err != nil {
			return fmt.Errorf("could not ensure events index: %w", err)
		}

		return nil
	}

	for i := range s.customPrefixes {
		prefix := s.customPrefixes[i]
		collection := s.db.Collection(prefix + "_events")

		if _, err := collection.Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys: bson.M{"aggregate_id": 1},
		}); err != nil {
			return fmt.Errorf("could not ensure events index: %w", err)
		}
	}

	return nil
}
