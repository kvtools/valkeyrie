package valkeyrie

import (
	"context"
	"testing"
	"time"

	"github.com/kvtools/valkeyrie/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStoreUnsupported(t *testing.T) {
	client := "localhost:9999"

	config := &store.Config{
		ConnectionTimeout: 10 * time.Second,
	}

	kv, err := NewStore(context.Background(), "unsupported", []string{client}, config)
	assert.Error(t, err)
	assert.Nil(t, kv)
	assert.Equal(t, "Backend storage not supported yet, please choose one of ", err.Error())
}

func TestListSupportedBackends(t *testing.T) {
	testCases := []struct {
		desc   string
		stores []string
		expect string
	}{
		{
			desc: "no store",
		},
		{
			desc:   "one store",
			stores: []string{"foo"},
			expect: "foo",
		},
		{
			desc:   "two unsorted stores",
			stores: []string{"foo", "bar"},
			expect: "bar, foo",
		},
		{
			desc:   "two sorted stores",
			stores: []string{"bar", "foo"},
			expect: "bar, foo",
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.desc, func(t *testing.T) {
			t.Parallel()
			t.Cleanup(func() { initializers = make(map[store.Backend]Initialize) })

			for _, s := range test.stores {
				AddStore(store.Backend(s), func(_ []string, _ *store.Config) (store.Store, error) { return nil, nil })

				kv, err := NewStore(store.Backend(s), nil, nil)
				require.NoError(t, err)
				require.Nil(t, kv) // AddStore return nil
			}
			assert.Equal(t, test.expect, supportedBackend())
		})
	}
}
