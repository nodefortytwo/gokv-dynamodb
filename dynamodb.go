package dynamodb

import (
	"context"
	"errors"
	"time"

	awsdynamodb "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	"github.com/philippgille/gokv/encoding"
	"github.com/philippgille/gokv/util"
)

// "k" is used as table column name for the key.
var keyAttrName = "k"

// "v" is used as table column name for the value.
var valAttrName = "v"

// Client is a gokv.Store implementation for DynamoDB.
type Client struct {
	svc       dynamodbiface.DynamoDBAPI
	tableName string
	codec     encoding.Codec
}

// Set stores the given value for the given key.
// Values are automatically marshalled to JSON or gob (depending on the configuration).
// The key must not be "" and the value must not be nil.
func (c Client) Set(k string, v interface{}) error {
	if err := util.CheckKeyAndValue(k, v); err != nil {
		return err
	}

	// First turn the passed object into something that DynamoDB can handle.
	data, err := c.codec.Marshal(v)
	if err != nil {
		return err
	}

	item := make(map[string]*awsdynamodb.AttributeValue)
	item[keyAttrName] = &awsdynamodb.AttributeValue{
		S: &k,
	}
	item[valAttrName] = &awsdynamodb.AttributeValue{
		B: data,
	}
	putItemInput := awsdynamodb.PutItemInput{
		TableName: &c.tableName,
		Item:      item,
	}
	_, err = c.svc.PutItem(&putItemInput)
	if err != nil {
		return err
	}
	return nil
}

// Get retrieves the stored value for the given key.
// You need to pass a pointer to the value, so in case of a struct
// the automatic unmarshalling can populate the fields of the object
// that v points to with the values of the retrieved object's values.
// If no value is found it returns (false, nil).
// The key must not be "" and the pointer must not be nil.
func (c Client) Get(k string, v interface{}) (found bool, err error) {
	if err := util.CheckKeyAndValue(k, v); err != nil {
		return false, err
	}

	key := make(map[string]*awsdynamodb.AttributeValue)
	key[keyAttrName] = &awsdynamodb.AttributeValue{
		S: &k,
	}
	getItemInput := awsdynamodb.GetItemInput{
		TableName: &c.tableName,
		Key:       key,
	}
	getItemOutput, err := c.svc.GetItem(&getItemInput)
	if err != nil {
		return false, err
	} else if getItemOutput.Item == nil {
		// Return false if the key-value pair doesn't exist
		return false, nil
	}
	attributeVal := getItemOutput.Item[valAttrName]
	if attributeVal == nil {
		// Return false if there's no value
		// TODO: Maybe return an error? Behaviour should be consistent across all implementations.
		return false, nil
	}
	data := attributeVal.B

	return true, c.codec.Unmarshal(data, v)
}

// Delete deletes the stored value for the given key.
// Deleting a non-existing key-value pair does NOT lead to an error.
// The key must not be "".
func (c Client) Delete(k string) error {
	if err := util.CheckKey(k); err != nil {
		return err
	}

	key := make(map[string]*awsdynamodb.AttributeValue)
	key[keyAttrName] = &awsdynamodb.AttributeValue{
		S: &k,
	}
	deleteItemInput := awsdynamodb.DeleteItemInput{
		TableName: &c.tableName,
		Key:       key,
	}
	_, err := c.svc.DeleteItem(&deleteItemInput)
	return err
}

// Close closes the client.
// In the DynamoDB implementation this doesn't have any effect.
func (c Client) Close() error {
	return nil
}

// Options are the options for the DynamoDB client.
type Options struct {
	TableName string
	Service dynamodbiface.DynamoDBAPI
	// Optional (encoding.JSON by default).
	Codec encoding.Codec
}

// DefaultOptions is an Options object with default values.
// Region: "" (use shared config file or environment variable), TableName: "gokv",
// AWSaccessKeyID: "" (use shared credentials file or environment variable),
// AWSsecretAccessKey: "" (use shared credentials file or environment variable),
// CustomEndpoint: "", Codec: encoding.JSON
var DefaultOptions = Options{
	Codec: encoding.JSON,
}

// NewClient creates a new DynamoDB client.
func NewClient(options Options) (Client, error) {
	result := Client{}

	if options.Service == nil {
		return result, errors.New("no dynamodb service provided")
	}
	result.svc = options.Service

	// Set default values
	if options.TableName == "" {
		return result, errors.New("no options.TableName specified")
	}
	result.tableName = options.TableName

	// Also serves as connection test.
	// Use context for timeout.
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	describeTableInput := awsdynamodb.DescribeTableInput{
		TableName: &options.TableName,
	}
	_, err := result.svc.DescribeTableWithContext(timeoutCtx, &describeTableInput)
	if err != nil {
		return result, err
	}

	result.codec = options.Codec

	return result, nil
}
