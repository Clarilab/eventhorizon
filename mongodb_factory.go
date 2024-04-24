package eventhorizon

import (
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type (
	// ClientFactory is a function that returns a mongo.Client.
	ClientFactory func() *mongo.Client
	// DatabaseFactory is a function that returns a mongo.Database.
	DatabaseFactory func() *mongo.Database
	// CollectionFactory is a function that returns a mongo.Collection by the given name.
	CollectionFactory func(name string, opts ...*options.CollectionOptions) *mongo.Collection
)

// MongoDBFactory is the interface for a MongoDB connection.
type MongoDBFactory interface {
	ClientFactory() ClientFactory
	DatabaseFactory() DatabaseFactory
	CollectionFactory() CollectionFactory
}

// MDBFactory is a basic implementation of the MongoDBFactory interface.
type MDBFactory struct {
	client   *mongo.Client
	database *mongo.Database
}

// NewMongoDBFactory creates a new MongoDB factory.
func NewMongoDBFactory(client *mongo.Client, dbName string) *MDBFactory {
	return &MDBFactory{
		client:   client,
		database: client.Database(dbName),
	}
}

// ClientFactory implements the MongoDBFactory interface.
func (c *MDBFactory) ClientFactory() ClientFactory {
	return func() *mongo.Client {
		return c.client
	}
}

// DatabaseFactory implements the MongoDBFactory interface.
func (c *MDBFactory) DatabaseFactory() DatabaseFactory {
	return func() *mongo.Database {
		return c.database
	}
}

// CollectionFactory implements the MongoDBFactory interface.
func (c *MDBFactory) CollectionFactory() CollectionFactory {
	return func(name string, opts ...*options.CollectionOptions) *mongo.Collection {
		return c.database.Collection(name)
	}
}
