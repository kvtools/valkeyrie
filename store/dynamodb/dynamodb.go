package dynamodb

import (
	"errors"
	"fmt"
	"github.com/abronan/valkeyrie"
	"github.com/abronan/valkeyrie/store"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	ddb "github.com/aws/aws-sdk-go/service/dynamodb"

	"strconv"
	"strings"
)

var (
	// ErrMultipleEndpointsUnsupported is thrown when there are
	// multiple endpoints specified for Dynamodb
	ErrMultipleEndpointsUnsupported = errors.New("DynamoDB does not support multiple endpoints")
)

// DynamoDB implements valkeyrie.Store interface with DynamoDB backend
// The primary key value of the table should a string named "Key"
// The attributes will be called "Index" and "Value"
type DynamoDB struct {
	tableName string
	client    *ddb.DynamoDB
}

// Register registers dynamodb to valkeyrie
func Register() {
	valkeyrie.AddStore(store.DYNAMODB, New)
}

// New create a new connection to dynamodb then table named endpoint
func New(endpoints []string, options *store.Config) (store.Store, error) {
	if len(endpoints) > 1 {
		return nil, ErrMultipleEndpointsUnsupported
	}

	// treate the bucket as the AWS region
	// default to us-east-1
	region := "us-east-1"
	if options.DynamodDBRegion != "" {
		region = options.DynamodDBRegion
	}

	var sess *session.Session
	var creds *credentials.Credentials

	// If creds are provided use those
	// Treate Username as AWS_ACCESS_KEY_ID and Password as AWS_SECRET_ACCESSK_EY
	if options.Username != "" && options.Password != "" {
		creds = credentials.NewStaticCredentials(options.Username, options.Password, "")
		// If region is set to localhost, we will parse the endpoint used for local dynamodb
		if region == "localhost" {
			sess, _ = session.NewSession(&aws.Config{
				Region:      aws.String(region),
				Endpoint:    aws.String(options.DynamodDBEndpoint),
				Credentials: creds,
			})
		} else {
			sess, _ = session.NewSession(&aws.Config{
				Region:      aws.String(region),
				Credentials: creds,
			})
		}
	} else {
		sess, _ = session.NewSession(&aws.Config{Region: aws.String(region)})
	}

	dyna := &DynamoDB{
		tableName: endpoints[0],
		client:    ddb.New(sess),
	}
	return dyna, nil
}

// Get gets the KVPair of the item stored at 'key' in the db
func (d *DynamoDB) Get(key string, opts *store.ReadOptions) (*store.KVPair, error) {

	params := &ddb.GetItemInput{
		Key: map[string]*ddb.AttributeValue{
			"Key": {
				S: aws.String(key),
			},
		},
		TableName: aws.String(d.tableName),
	}

	resp, err := d.client.GetItem(params)
	if err != nil {
		return nil, err
	}
	if len(resp.Item) == 0 {
		return nil, store.ErrKeyNotFound
	}

	pair := &store.KVPair{
		Key: key,
	}
	value, exists := resp.Item["Value"]
	if exists {
		pair.Value = []byte(*value.S)
	}
	pair.LastIndex, _ = strconv.ParseUint(*resp.Item["Index"].N, 10, 64)
	return pair, nil
}

// getPutParams returns an UpdateItemInput struct populated  depending
//  on if value is empty or not
func (d *DynamoDB) getPutParams(key string, value []byte, opts *store.WriteOptions) *ddb.UpdateItemInput {
	params := &ddb.UpdateItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]*ddb.AttributeValue{
			"Key": {
				S: aws.String(key),
			},
		},
		ReturnValues:     aws.String("ALL_NEW"),
		UpdateExpression: aws.String("add #i :i"),
		ExpressionAttributeNames: map[string]*string{
			"#i": aws.String("Index"),
		},
		ExpressionAttributeValues: map[string]*ddb.AttributeValue{
			":i": {
				N: aws.String("1"),
			},
		},
	}

	if len(value) != 0 {
		// DynamoDB doesn't allow empty values so remove the Value entirely
		params.ExpressionAttributeNames["#v"] = aws.String("Value")
		params.ExpressionAttributeValues[":v"] = &ddb.AttributeValue{S: aws.String(string(value[:]))}
		params.UpdateExpression = aws.String("set #v = :v add #i :i")
	}
	return params
}

// Put sets the 'key':'value' in the db
func (d *DynamoDB) Put(key string, value []byte, opts *store.WriteOptions) error {
	params := d.getPutParams(key, value, opts)
	_, err := d.client.UpdateItem(params)
	return err
}

// Delete deletes the 'key' from the db
func (d *DynamoDB) Delete(key string) error {
	params := &ddb.DeleteItemInput{
		Key: map[string]*ddb.AttributeValue{
			"Key": {
				S: aws.String(key),
			},
		},
		TableName: aws.String(d.tableName),
	}

	_, err := d.client.DeleteItem(params)
	return err
}

// Exists checks to see if the 'key' exists in the db
func (d *DynamoDB) Exists(key string, opts *store.ReadOptions) (bool, error) {
	pair, err := d.Get(key, opts)
	if pair != nil && pair.Key == "" || err == store.ErrKeyNotFound {
		return false, nil
	} else if err == nil {
		return true, nil
	}
	return false, err
}

// List gets all KVPairs whose keys start with 'directory'
func (d *DynamoDB) List(directory string, opts *store.ReadOptions) ([]*store.KVPair, error) {
	pairs := make([]*store.KVPair, 0)
	params := &ddb.ScanInput{
		FilterExpression: aws.String("begins_with( #k, :v)"),
		TableName:        aws.String(d.tableName),
		ExpressionAttributeNames: map[string]*string{
			"#k": aws.String("Key"),
		},
		ExpressionAttributeValues: map[string]*ddb.AttributeValue{
			":v": {
				S: aws.String(directory),
			},
		},
	}
	// TODO is scan the best way to do this?
	// Maybe a refactor of the key value format will allow
	// a more efficient query to be used or something?
	resp, err := d.client.Scan(params)
	if err != nil {
		return nil, err
	}
	// Scan won't throw an error if no items match the filter
	// so we check it
	if len(resp.Items) == 0 {
		return nil, store.ErrKeyNotFound
	}

	for _, item := range resp.Items {
		// Scan also returns the directory itself
		if *item["Key"].S == directory {
			continue
		}
		tPair := &store.KVPair{
			Key: *item["Key"].S,
		}
		// 'Value' may not exist for every key
		val, exists := item["Value"]
		if exists {
			tPair.Value = []byte(*val.S)
		}
		pairs = append(pairs, tPair)
	}
	return pairs, nil
}

// DeleteTree deletes all keys that start with 'directory'
func (d *DynamoDB) DeleteTree(directory string) error {
	retryList := make([]*store.KVPair, 0)
	pairs, err := d.List(directory, nil)
	if err != nil {
		return err
	}
	for _, pair := range pairs {
		err = d.Delete(pair.Key)
		if err != nil {
			retryList = append(retryList, pair)
		}
	}
	// TODO maybe retry deletes
	if len(retryList) > 0 {
		return fmt.Errorf("Unable to delete all of the tree: %v", retryList)
	}
	return nil
}

// Watch has to be implemented at the library level or be hooked up to a dynamodb stream
//   which might not be likely since AWS only suggests at most two processes reading
//   from a dynamodb stream
func (d *DynamoDB) Watch(key string, stopCh <-chan struct{}, opts *store.ReadOptions) (<-chan *store.KVPair, error) {
	// since scans are expensive maybe we should keep all keys being watched in a map
	// and consolidate our scans of the db into one scan
	return nil, errors.New("Watch not supported")
}

// WatchTree has to be implemented at the library since it is not natively supportedby dynamoDB
func (d *DynamoDB) WatchTree(directory string, stopCh <-chan struct{}, opts *store.ReadOptions) (<-chan []*store.KVPair, error) {
	return nil, errors.New("WatchTree not supported")
}

// NewLock is not supported
func (d *DynamoDB) NewLock(key string, opts *store.LockOptions) (store.Locker, error) {
	return nil, errors.New("NewLock not supported")
}

// AtomicPut put a value at "key" if the key has not been modified in the meantime
func (d *DynamoDB) AtomicPut(key string, value []byte, previous *store.KVPair, opts *store.WriteOptions) (bool, *store.KVPair, error) {
	params := d.getPutParams(key, value, opts)

	// if previous provided compare previous values to current values in DB
	if previous != nil {
		params.ConditionExpression = aws.String("#v = :pv AND #i = :pi")
		params.ExpressionAttributeValues[":pv"] = &ddb.AttributeValue{
			S: aws.String(string(previous.Value[:])),
		}
		params.ExpressionAttributeValues[":pi"] = &ddb.AttributeValue{
			N: aws.String(strconv.FormatUint(previous.LastIndex, 10)),
		}
	} else {
		// if previous not provided don't put if the item already exists
		params.ConditionExpression = aws.String("attribute_not_exists(#i)")
	}

	resp, err := d.client.UpdateItem(params)
	if err != nil {
		// check to see if the error was failure of the condition
		if strings.Contains(err.Error(), "ConditionalCheckFailedException") {
			if previous != nil {
				return false, nil, store.ErrKeyModified
			}
			return false, nil, store.ErrKeyExists
		}
		return false, nil, err
	}

	tmpIndex, _ := strconv.ParseUint(*resp.Attributes["Index"].N, 10, 64)
	// return new KVPair
	kvPairSuccess := &store.KVPair{
		Key:       key,
		Value:     value,
		LastIndex: tmpIndex,
	}
	return true, kvPairSuccess, nil
}

// AtomicDelete deletes the key if it hasn't been modified or if previous is not provided
func (d *DynamoDB) AtomicDelete(key string, previous *store.KVPair) (bool, error) {

	if previous == nil {
		if err := d.Delete(key); err != nil {
			return false, err
		}
		return true, nil
	}

	// DynamoDB has limited error handling, we need to manually check if a key exists
	keyExists, _ := d.Exists(key, nil)
	if !keyExists {
		return false, store.ErrKeyNotFound
	}
	// Delete if the indices match
	params := &ddb.DeleteItemInput{
		Key: map[string]*ddb.AttributeValue{
			"Key": {
				S: aws.String(key),
			},
		},
		TableName:           aws.String(d.tableName),
		ConditionExpression: aws.String("#i = :i"),
		ExpressionAttributeNames: map[string]*string{
			"#i": aws.String("Index"),
		},
		ExpressionAttributeValues: map[string]*ddb.AttributeValue{
			":i": {
				N: aws.String(strconv.FormatUint(previous.LastIndex, 10)),
			},
		},
	}

	// If there was a value in previous add
	if len(previous.Value) > 0 {
		params.ConditionExpression = aws.String("#v = :v AND #i = :i")
		params.ExpressionAttributeNames["#v"] = aws.String("Value")
		params.ExpressionAttributeValues[":v"] = &ddb.AttributeValue{S: aws.String(string(previous.Value[:]))}
	}

	_, err := d.client.DeleteItem(params)
	if err != nil {
		// check to see if it was the unmet condition
		if strings.Contains(err.Error(), "ConditionalCheckFailedException") {
			return false, store.ErrKeyModified
		}
		return false, err
	}
	return true, nil
}

// Close mock
func (d *DynamoDB) Close() {
	return
}
