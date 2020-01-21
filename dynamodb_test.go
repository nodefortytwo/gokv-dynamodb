package dynamodb

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type mockDynamoDB struct {
	dynamodbiface.DynamoDBAPI
}

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

// func TestNewClientHappyPath(t *testing.T) {
// 	opt := DefaultOptions
// 	opt.TableName = "awesome"
// 	opt.Service = mockDynamoDB{}
// 	_, err := NewClient(opt)
// 	if err != nil {
// 		t.Error(err)
// 	}
// }
//
// func TestNewClientMissingTable(t *testing.T) {
// 	opt := DefaultOptions
// 	opt.TableName = "missing"
// 	opt.Service = mockDynamoDB{}
// 	_, err := NewClient(opt)
// 	if err != nil {
// 		t.Error(err)
// 	}
// }

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