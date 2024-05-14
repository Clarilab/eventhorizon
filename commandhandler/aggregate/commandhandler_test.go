// Copyright (c) 2014 - The Event Horizon authors.
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

package aggregate

import (
	"context"
	"errors"
	"reflect"
	"testing"

	eh "github.com/Clarilab/eventhorizon"
	"github.com/Clarilab/eventhorizon/mocks"
	"github.com/Clarilab/eventhorizon/uuid"
)

func TestNewCommandHandler(t *testing.T) {
	store := &mocks.AggregateStore{
		Aggregates: make(map[uuid.UUID]eh.Aggregate),
		Snapshots:  make(map[uuid.UUID]eh.Snapshot),
	}

	h, err := NewCommandHandler(mocks.AggregateType, store)
	if err != nil {
		t.Error("there should be no error:", err)
	}

	if h == nil {
		t.Error("there should be a handler")
	}

	h, err = NewCommandHandler(mocks.AggregateType, nil)
	if !errors.Is(err, ErrNilAggregateStore) {
		t.Error("there should be a ErrNilAggregateStore error:", err)
	}

	if h != nil {
		t.Error("there should be no handler:", h)
	}
}

func TestCommandHandler(t *testing.T) {
	var (
		aggregate *mocks.Aggregate
		handler   *CommandHandler
	)

	test := func() {
		ctx := context.WithValue(context.Background(), "testkey", "testval")

		cmd := &mocks.Command{
			ID:      aggregate.EntityID(),
			Content: "command1",
		}

		err := handler.HandleCommand(ctx, cmd)
		if err != nil {
			t.Error("there should be no error:", err)
		}

		if !reflect.DeepEqual(aggregate.Commands, []eh.Command{cmd}) {
			t.Error("the handled command should be correct:", aggregate.Commands)
		}

		if val, ok := aggregate.Context.Value("testkey").(string); !ok || val != "testval" {
			t.Error("the context should be correct:", aggregate.Context)
		}
	}

	t.Run("non-atomic", func(t *testing.T) {
		aggregate, handler, _ = createAggregateAndHandler(t, false)

		test()
	})

	t.Run("atomic", func(t *testing.T) {
		aggregate, handler, _ = createAggregateAndHandler(t, true)

		test()
	})
}

func TestCommandHandler_AggregateNotFound(t *testing.T) {
	store := &mocks.AggregateStore{
		Aggregates: map[uuid.UUID]eh.Aggregate{},
		Snapshots:  make(map[uuid.UUID]eh.Snapshot),
	}

	h, err := NewCommandHandler(mocks.AggregateType, store)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	if h == nil {
		t.Fatal("there should be a handler")
	}

	cmd := &mocks.Command{
		ID:      uuid.New(),
		Content: "command1",
	}

	err = h.HandleCommand(context.Background(), cmd)
	if !errors.Is(err, eh.ErrAggregateNotFound) {
		t.Error("there should be a command error:", err)
	}
}

func TestCommandHandler_ErrorInHandler(t *testing.T) {
	a, h, _ := createAggregateAndHandler(t, false)

	commandErr := errors.New("command error")
	a.Err = commandErr
	cmd := &mocks.Command{
		ID:      a.EntityID(),
		Content: "command1",
	}
	aggregateErr := &eh.AggregateError{}

	err := h.HandleCommand(context.Background(), cmd)
	if !errors.As(err, &aggregateErr) || !errors.Is(err, commandErr) {
		t.Error("there should be a command error:", err)
	}

	if !reflect.DeepEqual(a.Commands, []eh.Command{}) {
		t.Error("the handled command should be correct:", a.Commands)
	}
}

func TestCommandHandler_ErrorWhenSaving(t *testing.T) {
	a, h, store := createAggregateAndHandler(t, false)

	saveErr := errors.New("save error")
	store.Err = saveErr
	cmd := &mocks.Command{
		ID:      a.EntityID(),
		Content: "command1",
	}

	err := h.HandleCommand(context.Background(), cmd)
	if !errors.Is(err, saveErr) {
		t.Error("there should be a command error:", err)
	}
}

func TestCommandHandler_NoHandlers(t *testing.T) {
	_, h, _ := createAggregateAndHandler(t, false)

	cmd := &mocks.Command{
		ID:      uuid.New(),
		Content: "command1",
	}

	err := h.HandleCommand(context.Background(), cmd)
	if !errors.Is(err, eh.ErrAggregateNotFound) {
		t.Error("there should be a ErrAggregateNotFound error:", nil)
	}
}

func BenchmarkCommandHandler(b *testing.B) {
	a := mocks.NewAggregate(uuid.New())
	store := &mocks.AggregateStore{
		Aggregates: map[uuid.UUID]eh.Aggregate{
			a.EntityID(): a,
		},
		Snapshots: make(map[uuid.UUID]eh.Snapshot),
	}

	h, err := NewCommandHandler(mocks.AggregateType, store)
	if err != nil {
		b.Fatal("there should be no error:", err)
	}

	ctx := context.WithValue(context.Background(), "testkey", "testval")

	cmd := &mocks.Command{
		ID:      a.EntityID(),
		Content: "command1",
	}
	for i := 0; i < b.N; i++ {
		h.HandleCommand(ctx, cmd)
	}

	if len(a.Commands) != b.N {
		b.Error("the num handled commands should be correct:", len(a.Commands), b.N)
	}
}

func BenchmarkCommandHandlerAtomic(b *testing.B) {
	a := mocks.NewAggregate(uuid.New())
	store := &mocks.AggregateStore{
		Aggregates: map[uuid.UUID]eh.Aggregate{
			a.EntityID(): a,
		},
		Snapshots: make(map[uuid.UUID]eh.Snapshot),
	}

	h, err := NewCommandHandler(mocks.AggregateType, store, WithUseAtomic())
	if err != nil {
		b.Fatal("there should be no error:", err)
	}

	ctx := context.WithValue(context.Background(), "testkey", "testval")

	cmd := &mocks.Command{
		ID:      a.EntityID(),
		Content: "command1",
	}
	for i := 0; i < b.N; i++ {
		h.HandleCommand(ctx, cmd)
	}

	if len(a.Commands) != b.N {
		b.Error("the num handled commands should be correct:", len(a.Commands), b.N)
	}
}

func createAggregateAndHandler(t *testing.T, useAtomic bool) (*mocks.Aggregate, *CommandHandler, *mocks.AggregateStore) {
	a := mocks.NewAggregate(uuid.New())
	store := &mocks.AggregateStore{
		Aggregates: map[uuid.UUID]eh.Aggregate{
			a.EntityID(): a,
		},
		Snapshots: make(map[uuid.UUID]eh.Snapshot),
	}

	if useAtomic {
		h, err := NewCommandHandler(mocks.AggregateType, store, WithUseAtomic())
		if err != nil {
			t.Fatal("there should be no error:", err)
		}

		return a, h, store
	}

	h, err := NewCommandHandler(mocks.AggregateType, store)
	if err != nil {
		t.Fatal("there should be no error:", err)
	}

	return a, h, store
}
