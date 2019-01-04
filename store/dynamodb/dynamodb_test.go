package dynamodb

import (
	"github.com/abronan/valkeyrie"
	"github.com/abronan/valkeyrie/store"
	"github.com/abronan/valkeyrie/testutils"
	"github.com/stretchr/testify/assert"
	"testing"
)

var client = "traefik"

func makeDynamoClient(t *testing.T) store.Store {
	kv, err := New(
		[]string{client},
		&store.Config{
			Username:          "foo",
			Password:          "bar",
			DynamodDBRegion:   "localhost",
			DynamodDBEndpoint: "http://127.0.0.1:8000",
		},
	)

	if err != nil {
		t.Fatalf("cannot create store: %v", err)
	}

	return kv
}

func TestRegister(t *testing.T) {
	Register()
	kv, err := valkeyrie.NewStore(store.DYNAMODB, []string{client}, &store.Config{})
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*DynamoDB); !ok {
		t.Fatal("Error registering and initializing dynamodb")
	}
}

func TestDynamoDBStore(t *testing.T) {
	kv := makeDynamoClient(t)
	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	testutils.RunCleanup(t, kv)
}
