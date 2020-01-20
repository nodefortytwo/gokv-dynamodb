package dynamodb

import (
	"testing"
)

func TestNewClient(t *testing.T) {
	opt := DefaultOptions
	opt.TableName = "awesome"
	_, err := NewClient(opt)
	if err != nil {
		t.Error(err)
	}
}