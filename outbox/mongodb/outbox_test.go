package mongodb_test

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"time"

	"os"
	"testing"

	"github.com/looplab/eventhorizon/outbox"
	"github.com/looplab/eventhorizon/outbox/mongodb"
)

// func init() {
// 	rand.Seed(time.Now().Unix())
// }

func TestOutboxAddHandler(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	uri, dbName := makeDB(t)

	obx, err := mongodb.NewOutbox(uri, dbName)
	if err != nil {
		t.Fatal(err)
	}

	outbox.TestAddHandler(t, obx, context.Background())
}

func TestOutboxIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	uri, dbName := makeDB(t)

	obx, err := mongodb.NewOutbox(uri, dbName,
		// Shorter sweeps for testing
		mongodb.WithPeriodicSweepInterval(2*time.Second),
		mongodb.WithPeriodicSweepAge(2*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}

	obx.Start()

	outbox.AcceptanceTest(t, obx, context.Background(), "none")

	if err := obx.Close(); err != nil {
		t.Error("there should be no error:", err)
	}
}

func TestWithCollectionNameIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	uri, dbName := makeDB(t)

	obx, err := mongodb.NewOutbox(uri, dbName, mongodb.WithCollectionName("foo-outbox"))
	if err != nil {
		t.Fatal(err)
	}

	defer obx.Close()

	if obx == nil {
		t.Fatal("there should be a store")
	}

	if obx.OutboxCollectionName() != "foo-outbox" {
		t.Fatal("collection name should use custom collection name")
	}
}

func TestWithCollectionNameInvalidNames(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	uri, dbName := makeDB(t)

	nameWithSpaces := "foo outbox"

	_, err := mongodb.NewOutbox(uri, dbName, mongodb.WithCollectionName(nameWithSpaces))
	if err == nil || err.Error() != "error while applying option: outbox collection: invalid char in collection name (space)" {
		t.Fatal("there should be an error")
	}

	_, err = mongodb.NewOutbox(uri, dbName, mongodb.WithCollectionName(""))
	if err == nil || err.Error() != "error while applying option: outbox collection: missing collection name" {
		t.Fatal("there should be an error")
	}
}

func makeDB(t *testing.T) (string, string) {
	t.Helper()

	// Use MongoDB in Docker with fallback to localhost.
	url := os.Getenv("MONGODB_ADDR")
	if url == "" {
		url = "localhost:27017"
	}

	url = "mongodb://" + url

	// Get a random DB name.
	bs := make([]byte, 4)
	if _, err := cryptorand.Read(bs); err != nil {
		t.Fatal(err)
	}

	dbName := "test-" + hex.EncodeToString(bs)

	t.Log("using DB:", dbName)

	return url, dbName
}

func BenchmarkOutbox(b *testing.B) {
	// Use MongoDB in Docker with fallback to localhost.
	url := os.Getenv("MONGODB_ADDR")
	if url == "" {
		url = "localhost:27017"
	}

	url = "mongodb://" + url

	// Get a random DB name.
	bs := make([]byte, 4)
	if _, err := cryptorand.Read(bs); err != nil {
		b.Fatal(err)
	}

	dbName := "test-" + hex.EncodeToString(bs)

	b.Log("using DB:", dbName)

	obx, err := mongodb.NewOutbox(url, dbName,
		// Shorter sweeps for testing.
		mongodb.WithPeriodicSweepInterval(1*time.Second),
		mongodb.WithPeriodicSweepAge(1*time.Second),
		mongodb.WithPeriodicCleanupAge(5*time.Second),
	)
	if err != nil {
		b.Fatal(err)
	}

	obx.Start()

	outbox.Benchmark(b, obx)

	if err := obx.Close(); err != nil {
		b.Error("there should be no error:", err)
	}
}
