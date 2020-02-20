package dynamodb

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	. "github.com/onsi/gomega"
)

type mockDynamoDB struct {
	dynamodbiface.DynamoDBAPI

}

var putItems = map[string]*dynamodb.PutItemInput{}

func (m mockDynamoDB) DescribeTableWithContext(c context.Context, i *dynamodb.DescribeTableInput, ro...request.Option) (*dynamodb.DescribeTableOutput, error) {

	switch aws.StringValue(i.TableName) {
	case "missing":
		return nil, dynamodb.TableNotFoundException{}
	}

	return &dynamodb.DescribeTableOutput{
		Table: &dynamodb.TableDescription{
			TableName: i.TableName,
		},
	}, nil
}

func (m mockDynamoDB) PutItem(i *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error){
	key := i.Item[keyAttrName].S

	putItems[*key] = i
	return nil, nil
}




func TestNewClient(t *testing.T) {
	type args struct {
		options Options
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:"happy-path",
			args: args{
				options: Options{
					Service:   mockDynamoDB{},
					TableName: "happy-path",
				},
			},
			wantErr: false,
		},
		{
			name:"missing",
			args: args{
				options: Options{
					Service:   mockDynamoDB{},
					TableName: "missing",
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewClient(tt.args.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestTTL(t *testing.T) {
	g := NewGomegaWithT(t)
	s := mockDynamoDB{}
	type args struct {
		options Options
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:"nottl",
			args: args{
				options: Options{
					Service:   s,
					TableName: "nottl",
				},
			},
			wantErr: false,
		},
		{
			name:"withttl",
			args: args{
				options: Options{
					Service:   s,
					TableName: "withttl",
					TTL: time.Second * 10,
				},
			},
			wantErr: false,
		},
	}


	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := NewClient(tt.args.options)
			c.Set(tt.name, "awesome")

			switch tt.name{
			case "nottl":
				g.Expect(putItems[tt.name].Item).NotTo(HaveKey("ttl"))

			case"withttl":
				g.Expect(putItems[tt.name].Item).To(HaveKey("ttl"))

			}
		})
	}
}
