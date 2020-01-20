package dynamodb

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awsdynamodb "github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/philippgille/gokv/encoding"
	"github.com/philippgille/gokv/util"
)

// "k" is used as table column name for the key.
var keyAttrName = "k"

// "v" is used as table column name for the value.
var valAttrName = "v"

// Client is a gokv.Store implementation for DynamoDB.
type Client struct {
	svc       *awsdynamodb.DynamoDB
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
	_, err := c.c.DeleteItem(&deleteItemInput)
	return err
}

// Close closes the client.
// In the DynamoDB implementation this doesn't have any effect.
func (c Client) Close() error {
	return nil
}

// Options are the options for the DynamoDB client.
type Options struct {
	Session   *session.Session
	AWSConfig *aws.Config
	TableName string
	// CustomEndpoint allows you to set a custom DynamoDB service endpoint.
	// This is especially useful if you're running a "DynamoDB local" Docker container for local testing.
	// Typical value for the Docker container: "http://localhost:8000".
	// See https://hub.docker.com/r/amazon/dynamodb-local/.
	// Optional ("" by default)
	CustomEndpoint string
	// Encoding format.
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
//
// Credentials can be set in the options, but it's recommended to either use the shared credentials file
// (Linux: "~/.aws/credentials", Windows: "%UserProfile%\.aws\credentials")
// or environment variables (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY).
// See https://github.com/awsdocs/aws-go-developer-guide/blob/0ae5712d120d43867cf81de875cb7505f62f2d71/doc_source/configuring-sdk.rst#specifying-credentials.
func NewClient(options Options) (Client, error) {
	result := Client{}

	// Set default values
	if options.TableName == "" {
		return result, errors.New("no options.TableName specified")
	}
	if options.Codec == nil {
		options.Codec = DefaultOptions.Codec
	}

	if options.AWSConfig == nil {
		options.AWSConfig = &aws.Config{}
	}

	if options.Session == nil {
		options.Session = session.Must(session.NewSession())
	}

	svc := awsdynamodb.New(options.Session, options.AWSConfig)

	// Create table if it doesn't exist.
	// Also serves as connection test.
	// Use context for timeout.
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	describeTableInput := awsdynamodb.DescribeTableInput{
		TableName: &options.TableName,
	}
	_, err = svc.DescribeTableWithContext(timeoutCtx, &describeTableInput)
	if err != nil {
		return result, err
	}

	result.svc = svc
	result.tableName = options.TableName
	result.codec = options.Codec

	return result, nil
}
