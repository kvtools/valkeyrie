// Package dynamodb contains the DynamoDB store implementation.
package dynamodb

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/kvtools/valkeyrie"
	"github.com/kvtools/valkeyrie/store"
)

const (
	// DefaultReadCapacityUnits default read capacity used to create table.
	DefaultReadCapacityUnits = 2
	// DefaultWriteCapacityUnits default write capacity used to create table.
	DefaultWriteCapacityUnits = 2
	// TableCreateTimeoutSeconds the maximum time we wait for the AWS DynamoDB table to be created.
	TableCreateTimeoutSeconds = 30
	// DeleteTreeTimeoutSeconds the maximum time we retry a write batch.
	DeleteTreeTimeoutSeconds = 30
)

const (
	partitionKey          = "id"
	revisionAttribute     = "version"
	encodedValueAttribute = "encoded_value"
	ttlAttribute          = "expiration_time"
)

const (
	defaultLockTTL         = 20 * time.Second
	dynamodbDefaultTimeout = 10 * time.Second
)

var (
	// ErrBucketOptionMissing is returned when bucket config option is missing.
	ErrBucketOptionMissing = errors.New("missing dynamodb bucket/table name")
	// ErrMultipleEndpointsUnsupported is returned when more than one endpoint is provided.
	ErrMultipleEndpointsUnsupported = errors.New("dynamodb only supports one endpoint")
	// ErrTableCreateTimeout table creation timed out.
	ErrTableCreateTimeout = errors.New("dynamodb table creation timed out")
	// ErrDeleteTreeTimeout delete batch timed out.
	ErrDeleteTreeTimeout = errors.New("delete batch timed out")
	// ErrLockAcquireCancelled stop called before lock was acquired.
	ErrLockAcquireCancelled = errors.New("stop called before lock was acquired")
)

// Register register a store provider in valkeyrie for AWS DynamoDB.
func Register() {
	valkeyrie.AddStore(store.DYNAMODB, New)
}

// New opens and creates a new table.
func New(endpoints []string, options *store.Config) (store.Store, error) {
	if len(endpoints) > 1 {
		return nil, ErrMultipleEndpointsUnsupported
	}

	if options == nil || options.Bucket == "" {
		return nil, ErrBucketOptionMissing
	}
	var config *aws.Config
	if len(endpoints) == 1 {
		config = &aws.Config{
			Endpoint: aws.String(endpoints[0]),
		}
	}

	ddb := &DynamoDB{
		dynamoSvc: dynamodb.New(session.Must(session.NewSession(config))),
		tableName: options.Bucket,
	}

	return ddb, nil
}

// DynamoDB store used to interact with AWS DynamoDB.
type DynamoDB struct {
	dynamoSvc dynamodbiface.DynamoDBAPI
	tableName string
}

// Put a value at the specified key.
func (ddb *DynamoDB) Put(ctx context.Context, key string, value []byte, opts *store.WriteOptions) error {
	keyAttr := make(map[string]*dynamodb.AttributeValue)
	keyAttr[partitionKey] = &dynamodb.AttributeValue{S: aws.String(key)}

	exAttr := map[string]*dynamodb.AttributeValue{
		":incr": {N: aws.String("1")},
	}

	var setList []string

	// if a value was provided append it to the update expression.
	if len(value) > 0 {
		encodedValue := base64.StdEncoding.EncodeToString(value)
		exAttr[":encv"] = &dynamodb.AttributeValue{S: aws.String(encodedValue)}
		setList = append(setList, fmt.Sprintf("%s = :encv", encodedValueAttribute))
	}

	// if a ttl was provided validate it and append it to the update expression.
	if opts != nil && opts.TTL > 0 {
		ttlVal := time.Now().Add(opts.TTL).Unix()
		exAttr[":ttl"] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(ttlVal, 10))}
		setList = append(setList, fmt.Sprintf("%s = :ttl", ttlAttribute))
	}

	updateExp := fmt.Sprintf("ADD %s :incr", revisionAttribute)

	if len(setList) > 0 {
		updateExp = fmt.Sprintf("%s SET %s", updateExp, strings.Join(setList, ","))
	}

	_, err := ddb.dynamoSvc.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(ddb.tableName),
		Key:                       keyAttr,
		ExpressionAttributeValues: exAttr,
		UpdateExpression:          aws.String(updateExp),
	})
	if err != nil {
		return err
	}

	return nil
}

// Get a value given its key.
func (ddb *DynamoDB) Get(ctx context.Context, key string, opts *store.ReadOptions) (*store.KVPair, error) {
	if opts == nil {
		opts = &store.ReadOptions{
			Consistent: true, // default to enabling read consistency.
		}
	}

	res, err := ddb.getKey(ctx, key, opts)
	if err != nil {
		return nil, err
	}
	if res.Item == nil {
		return nil, store.ErrKeyNotFound
	}

	// is the item expired?
	if isItemExpired(res.Item) {
		return nil, store.ErrKeyNotFound
	}

	return decodeItem(res.Item)
}

func (ddb *DynamoDB) getKey(ctx context.Context, key string, options *store.ReadOptions) (*dynamodb.GetItemOutput, error) {
	return ddb.dynamoSvc.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(ddb.tableName),
		ConsistentRead: aws.Bool(options.Consistent),
		Key: map[string]*dynamodb.AttributeValue{
			partitionKey: {S: aws.String(key)},
		},
	})
}

// Delete the value at the specified key.
func (ddb *DynamoDB) Delete(ctx context.Context, key string) error {
	_, err := ddb.dynamoSvc.DeleteItemWithContext(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(ddb.tableName),
		Key: map[string]*dynamodb.AttributeValue{
			partitionKey: {S: aws.String(key)},
		},
	})
	if err != nil {
		return err
	}

	return nil
}

// Exists if a Key exists in the store.
func (ddb *DynamoDB) Exists(ctx context.Context, key string, opts *store.ReadOptions) (bool, error) {
	res, err := ddb.dynamoSvc.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(ddb.tableName),
		Key: map[string]*dynamodb.AttributeValue{
			partitionKey: {
				S: aws.String(key),
			},
		},
	})
	if err != nil {
		return false, err
	}

	if res.Item == nil {
		return false, nil
	}

	// is the item expired?
	if isItemExpired(res.Item) {
		return false, nil
	}

	return true, nil
}

// List the content of a given prefix.
func (ddb *DynamoDB) List(ctx context.Context, directory string, opts *store.ReadOptions) ([]*store.KVPair, error) {
	if opts == nil {
		opts = &store.ReadOptions{
			Consistent: true, // default to enabling read consistency.
		}
	}

	expAttr := make(map[string]*dynamodb.AttributeValue)
	expAttr[":namePrefix"] = &dynamodb.AttributeValue{S: aws.String(directory)}

	filterExp := fmt.Sprintf("begins_with(%s, :namePrefix)", partitionKey)

	si := &dynamodb.ScanInput{
		TableName:                 aws.String(ddb.tableName),
		FilterExpression:          aws.String(filterExp),
		ExpressionAttributeValues: expAttr,
		ConsistentRead:            aws.Bool(opts.Consistent),
	}

	var items []map[string]*dynamodb.AttributeValue
	ctx, cancel := context.WithTimeout(ctx, dynamodbDefaultTimeout)

	err := ddb.dynamoSvc.ScanPagesWithContext(ctx, si,
		func(page *dynamodb.ScanOutput, lastPage bool) bool {
			items = append(items, page.Items...)

			if lastPage {
				cancel()
				return false
			}

			return true
		})
	if err != nil {
		return nil, err
	}

	if len(items) == 0 {
		return nil, store.ErrKeyNotFound
	}

	kvArray := []*store.KVPair{}
	var val *store.KVPair

	for _, item := range items {
		val, err = decodeItem(item)
		if err != nil {
			return nil, err
		}

		// skip the records which match the prefix.
		if val.Key == directory {
			continue
		}
		// skip records which are expired.
		if isItemExpired(item) {
			continue
		}

		kvArray = append(kvArray, val)
	}

	return kvArray, nil
}

// DeleteTree deletes a range of keys under a given directory.
func (ddb *DynamoDB) DeleteTree(ctx context.Context, keyPrefix string) error {
	expAttr := make(map[string]*dynamodb.AttributeValue)

	expAttr[":namePrefix"] = &dynamodb.AttributeValue{S: aws.String(keyPrefix)}

	res, err := ddb.dynamoSvc.ScanWithContext(ctx, &dynamodb.ScanInput{
		TableName:                 aws.String(ddb.tableName),
		FilterExpression:          aws.String(fmt.Sprintf("begins_with(%s, :namePrefix)", partitionKey)),
		ExpressionAttributeValues: expAttr,
	})
	if err != nil {
		return err
	}

	if len(res.Items) == 0 {
		return nil
	}

	items := make(map[string][]*dynamodb.WriteRequest)

	items[ddb.tableName] = make([]*dynamodb.WriteRequest, len(res.Items))

	for n, item := range res.Items {
		items[ddb.tableName][n] = &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: map[string]*dynamodb.AttributeValue{
					partitionKey: item[partitionKey],
				},
			},
		}
	}

	return ddb.retryDeleteTree(ctx, items)
}

// AtomicPut Atomic CAS operation on a single value.
func (ddb *DynamoDB) AtomicPut(ctx context.Context, key string, value []byte, previous *store.KVPair, opts *store.WriteOptions) (bool, *store.KVPair, error) {
	getRes, err := ddb.getKey(ctx, key, &store.ReadOptions{
		Consistent: true, // enable the read consistent flag.
	})
	if err != nil {
		return false, nil, err
	}

	// AtomicPut is equivalent to Put if previous is nil and the Key exist in the DB or is not expired.
	if previous == nil && getRes.Item != nil && !isItemExpired(getRes.Item) {
		return false, nil, store.ErrKeyExists
	}

	keyAttr := make(map[string]*dynamodb.AttributeValue)
	keyAttr[partitionKey] = &dynamodb.AttributeValue{S: aws.String(key)}

	exAttr := make(map[string]*dynamodb.AttributeValue)
	exAttr[":incr"] = &dynamodb.AttributeValue{N: aws.String("1")}

	var setList []string

	// if a value was provided append it to the update expression.
	if len(value) > 0 {
		encodedValue := base64.StdEncoding.EncodeToString(value)
		exAttr[":encv"] = &dynamodb.AttributeValue{S: aws.String(encodedValue)}
		setList = append(setList, fmt.Sprintf("%s = :encv", encodedValueAttribute))
	}

	// if a ttl was provided validate it and append it to the update expression.
	if opts != nil && opts.TTL > 0 {
		ttlVal := time.Now().Add(opts.TTL).Unix()
		exAttr[":ttl"] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(ttlVal, 10))}
		setList = append(setList, fmt.Sprintf("%s = :ttl", ttlAttribute))
	}

	updateExp := fmt.Sprintf("ADD %s :incr", revisionAttribute)

	if len(setList) > 0 {
		updateExp = fmt.Sprintf("%s SET %s", updateExp, strings.Join(setList, ","))
	}

	var condExp *string

	if previous != nil {
		exAttr[":lastRevision"] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatUint(previous.LastIndex, 10))}
		exAttr[":timeNow"] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(time.Now().Unix(), 10))}

		// the previous kv is in the DB and is at the expected revision, also if it has a TTL set it is NOT expired.
		condExp = aws.String(fmt.Sprintf("%s = :lastRevision AND (attribute_not_exists(%s) OR (attribute_exists(%s) AND %s > :timeNow))",
			revisionAttribute, ttlAttribute, ttlAttribute, ttlAttribute))
	}

	res, err := ddb.dynamoSvc.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(ddb.tableName),
		Key:                       keyAttr,
		ExpressionAttributeValues: exAttr,
		UpdateExpression:          aws.String(updateExp),
		ConditionExpression:       condExp,
		ReturnValues:              aws.String(dynamodb.ReturnValueAllNew),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return false, nil, store.ErrKeyModified
			}
		}
		return false, nil, err
	}

	item, err := decodeItem(res.Attributes)
	if err != nil {
		return false, nil, err
	}

	return true, item, nil
}

// AtomicDelete delete of a single value.
func (ddb *DynamoDB) AtomicDelete(ctx context.Context, key string, previous *store.KVPair) (bool, error) {
	getRes, err := ddb.getKey(ctx, key, &store.ReadOptions{
		Consistent: true, // enable the read consistent flag.
	})
	if err != nil {
		return false, err
	}

	if previous == nil && getRes.Item != nil && !isItemExpired(getRes.Item) {
		return false, store.ErrKeyExists
	}

	expAttr := make(map[string]*dynamodb.AttributeValue)
	if previous != nil {
		expAttr[":lastRevision"] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatUint(previous.LastIndex, 10))}
	}

	req := &dynamodb.DeleteItemInput{
		TableName: aws.String(ddb.tableName),
		Key: map[string]*dynamodb.AttributeValue{
			partitionKey: {S: aws.String(key)},
		},
		ConditionExpression:       aws.String(fmt.Sprintf("%s = :lastRevision", revisionAttribute)),
		ExpressionAttributeValues: expAttr,
	}

	_, err = ddb.dynamoSvc.DeleteItemWithContext(ctx, req)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return false, store.ErrKeyNotFound
			}
		}
		return false, err
	}

	return true, nil
}

// Close nothing to see here.
func (ddb *DynamoDB) Close() {}

// NewLock has to implemented at the library level since it's not supported by DynamoDB.
func (ddb *DynamoDB) NewLock(key string, options *store.LockOptions) (store.Locker, error) {
	ttl := defaultLockTTL
	if options != nil && options.TTL != 0 {
		ttl = options.TTL
	}

	var value []byte
	if options != nil && len(options.Value) != 0 {
		value = options.Value
	}

	renewCh := make(chan struct{})
	if options != nil && options.RenewLock != nil {
		renewCh = options.RenewLock
	}

	return &dynamodbLock{
		ddb:      ddb,
		last:     nil,
		key:      key,
		value:    value,
		ttl:      ttl,
		renewCh:  renewCh,
		unlockCh: make(chan struct{}),
	}, nil
}

// Watch has to implemented at the library level since it's not supported by DynamoDB.
func (ddb *DynamoDB) Watch(_ context.Context, _ string, _ <-chan struct{}, _ *store.ReadOptions) (<-chan *store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

// WatchTree has to implemented at the library level since it's not supported by DynamoDB.
func (ddb *DynamoDB) WatchTree(_ context.Context, _ string, _ <-chan struct{}, _ *store.ReadOptions) (<-chan []*store.KVPair, error) {
	return nil, store.ErrCallNotSupported
}

func (ddb *DynamoDB) createTable() error {
	_, err := ddb.dynamoSvc.CreateTable(&dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(partitionKey),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(partitionKey),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
		},
		// enable encryption of data by default.
		SSESpecification: &dynamodb.SSESpecification{
			Enabled: aws.Bool(true),
			SSEType: aws.String(dynamodb.SSETypeAes256),
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(DefaultReadCapacityUnits),
			WriteCapacityUnits: aws.Int64(DefaultWriteCapacityUnits),
		},
		TableName: aws.String(ddb.tableName),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeResourceInUseException {
				return nil
			}
		}
		return err
	}

	err = ddb.dynamoSvc.WaitUntilTableExists(&dynamodb.DescribeTableInput{
		TableName: aws.String(ddb.tableName),
	})
	if err != nil {
		return err
	}

	return nil
}

func (ddb *DynamoDB) retryDeleteTree(ctx context.Context, items map[string][]*dynamodb.WriteRequest) error {
	batchResult, err := ddb.dynamoSvc.BatchWriteItemWithContext(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: items,
	})
	if err != nil {
		return err
	}

	if len(batchResult.UnprocessedItems) == 0 {
		return nil
	}

	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(DeleteTreeTimeoutSeconds * time.Second)
		timeout <- true
	}()

	ticker := time.NewTicker(1 * time.Second)

	defer ticker.Stop()

	// Poll once a second for table status,
	// until the table is either active or the timeout deadline has been reached.
	for {
		select {
		case <-ticker.C:
			batchResult, err = ddb.dynamoSvc.BatchWriteItemWithContext(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: batchResult.UnprocessedItems,
			})
			if err != nil {
				return err
			}

			if len(batchResult.UnprocessedItems) == 0 {
				return nil
			}

		case <-timeout:
			// polling for table status has taken more than the timeout.
			return ErrDeleteTreeTimeout
		}
	}
}

type dynamodbLock struct {
	ddb      *DynamoDB
	last     *store.KVPair
	renewCh  chan struct{}
	unlockCh chan struct{}

	key   string
	value []byte
	ttl   time.Duration
}

func (l *dynamodbLock) Lock(stopChan chan struct{}) (<-chan struct{}, error) {
	lockHeld := make(chan struct{})

	ctx := context.Background()

	success, err := l.tryLock(ctx, lockHeld, stopChan)
	if err != nil {
		return nil, err
	}
	if success {
		return lockHeld, nil
	}

	// TODO: This really needs a jitter for backoff.
	ticker := time.NewTicker(3 * time.Second)

	for {
		select {
		case <-ticker.C:
			success, err := l.tryLock(ctx, lockHeld, stopChan)
			if err != nil {
				return nil, err
			}
			if success {
				return lockHeld, nil
			}
		case <-stopChan:
			return nil, ErrLockAcquireCancelled
		}
	}
}

func (l *dynamodbLock) Unlock() error {
	l.unlockCh <- struct{}{}

	_, err := l.ddb.AtomicDelete(context.Background(), l.key, l.last)
	if err != nil {
		return err
	}
	l.last = nil

	return nil
}

func (l *dynamodbLock) tryLock(ctx context.Context, lockHeld chan struct{}, stopChan chan struct{}) (bool, error) {
	success, item, err := l.ddb.AtomicPut(ctx, l.key, l.value, l.last, &store.WriteOptions{TTL: l.ttl})
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) || errors.Is(err, store.ErrKeyModified) || errors.Is(err, store.ErrKeyExists) {
			return false, nil
		}
		return false, err
	}
	if success {
		l.last = item
		// keep holding.
		go l.holdLock(ctx, lockHeld, stopChan)
		return true, nil
	}

	return false, err
}

func (l *dynamodbLock) holdLock(ctx context.Context, lockHeld, stopChan chan struct{}) {
	defer close(lockHeld)

	hold := func() error {
		_, item, err := l.ddb.AtomicPut(ctx, l.key, l.value, l.last, &store.WriteOptions{TTL: l.ttl})
		if err != nil {
			return err
		}

		l.last = item
		return nil
	}

	// may need a floor of 1 second set.
	heartbeat := time.NewTicker(l.ttl / 3)
	defer heartbeat.Stop()

	for {
		select {
		case <-heartbeat.C:
			if err := hold(); err != nil {
				return
			}
		case <-l.renewCh:
			return
		case <-l.unlockCh:
			return
		case <-stopChan:
			return
		}
	}
}

func isItemExpired(item map[string]*dynamodb.AttributeValue) bool {
	v, ok := item[ttlAttribute]
	if !ok {
		return false
	}

	ttl, _ := strconv.ParseInt(aws.StringValue(v.N), 10, 64)
	return time.Unix(ttl, 0).Before(time.Now())
}

func decodeItem(item map[string]*dynamodb.AttributeValue) (*store.KVPair, error) {
	var key string
	if v, ok := item[partitionKey]; ok {
		key = aws.StringValue(v.S)
	}

	var revision int64
	if v, ok := item[revisionAttribute]; ok {
		var err error
		revision, err = strconv.ParseInt(aws.StringValue(v.N), 10, 64)
		if err != nil {
			return nil, err
		}
	}

	var encodedValue string
	if v, ok := item[encodedValueAttribute]; ok {
		encodedValue = aws.StringValue(v.S)
	}

	rawValue, err := base64.StdEncoding.DecodeString(encodedValue)
	if err != nil {
		return nil, err
	}

	return &store.KVPair{
		Key:       key,
		Value:     rawValue,
		LastIndex: uint64(revision),
	}, nil
}
