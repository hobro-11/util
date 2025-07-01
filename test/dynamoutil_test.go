package test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/assert"

	"github.com/gofiber/fiber/v2/log"
	"github.com/hobro-11/util/dynamoutil"
	dynamo_err "github.com/hobro-11/util/dynamoutil/errors"
)

// TODO: set your aws region, access key id, secret access key, table name
var (
	client             *dynamodb.Client
	awsRegion          = ""
	awsAccessKeyID     = ""
	awsSecretAccessKey = ""
	tableName          = ""
)

func init() {
	if awsRegion == "" || awsAccessKeyID == "" || awsSecretAccessKey == "" || tableName == "" {
		log.Fatal("awsRegion, awsAccessKeyID, awsSecretAccessKey, tableName is empty")
		return
	}

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				awsAccessKeyID,
				awsSecretAccessKey,
				"")),
		config.WithRegion(awsRegion),
	)
	if err != nil {
		log.Errorf("LoadDefaultConfig failed: %v", err)
	}

	client = dynamodb.NewFromConfig(cfg)
}

func deleteItem() {
	keys := dynamoutil.Keys{
		PK:     "pk",
		PKName: "pk",
		SK:     "sk",
		SKName: "sk",
	}

	deleteArg := dynamoutil.NewDeleteArg(tableName, keys, "")
	if err := dynamoutil.DeleteItem(context.Background(), client, deleteArg); err != nil {
		log.Errorf("Error deleting item: %v", err)
	}
}

type (
	stubPutItem struct {
		PartitionKey string `dynamodbav:"pk"`
		SortKey      string `dynamodbav:"sk"`
		Title        string `dynamodbav:"title"`
	}

	stubGetItem struct {
		PartitionKey string `dynamodbav:"pk"`
		SortKey      string `dynamodbav:"sk"`
		Title        string `dynamodbav:"title"`
	}
)

func TestPutAndGet(t *testing.T) {
	item := stubPutItem{
		PartitionKey: "pk",
		SortKey:      "sk",
		Title:        "title",
	}

	putArg := dynamoutil.NewPutArg(tableName, item, "attribute_not_exists(pk) AND attribute_not_exists(sk)")

	if err := dynamoutil.PutItem(context.Background(), client, putArg); err != nil {
		t.Errorf("Error putting item: %v", err)
	}

	getArg := dynamoutil.NewGetArg(tableName, dynamoutil.Keys{
		PK:     "pk",
		PKName: "pk",
		SK:     "sk",
		SKName: "sk",
	})

	result, err := dynamoutil.GetItem[stubGetItem](context.Background(), client, getArg)
	if err != nil {
		t.Errorf("Error getting item: %v", err)
	}
	if result == nil {
		t.Errorf("Item not found")
	} else if result.PartitionKey != "pk" || result.SortKey != "sk" || result.Title != "title" {
		t.Errorf("Item not matched")
	}

	deleteItem()
}

func TestPutConditionFailed(t *testing.T) {
	item := stubPutItem{
		PartitionKey: "pk",
		SortKey:      "sk",
		Title:        "title",
	}

	putArg := dynamoutil.NewPutArg(tableName, item, "attribute_not_exists(pk) AND attribute_not_exists(sk)")

	getArg := dynamoutil.NewGetArg(tableName, dynamoutil.Keys{
		PK:     "pk",
		PKName: "pk",
		SK:     "sk",
		SKName: "sk",
	})

	task := func() error {

		err := dynamoutil.PutItem(context.Background(), client, putArg)
		if err != nil {
			return err
		}

		result, err := dynamoutil.GetItem[stubGetItem](context.Background(), client, getArg)
		if err != nil {
			t.Errorf("Error getting item: %v", err)
		}
		if result == nil {
			t.Errorf("Item not found")
		} else if result.PartitionKey != "pk" || result.SortKey != "sk" || result.Title != "title" {
			t.Errorf("Item not matched")
		}
		return nil
	}

	task()
	err := task()
	var failErr *dynamo_err.ApiError
	errors.As(err, &failErr)
	assert.Equal(t, dynamo_err.ConditionalCheckFailedException, failErr.Message)
	deleteItem()
}

type stubValidationErrPutItem struct {
	PartitionKey string `dynamodbav:"pk"`
	// this is not exists sort_key
	SortKey string `dynamodbav:"s"`
	Title   string `dynamodbav:"title"`
}

func TestValidationErr(t *testing.T) {
	item := stubValidationErrPutItem{
		PartitionKey: "pk",
		SortKey:      "sk",
		Title:        "title",
	}

	putArg := dynamoutil.NewPutArg(tableName, item, "attribute_not_exists(pk) AND attribute_not_exists(sk)")

	err := dynamoutil.PutItem(context.Background(), client, putArg)
	var failErr *dynamo_err.ApiError
	errors.As(err, &failErr)
	assert.Equal(t, dynamo_err.ValidationException, failErr.Message)
}

func TestUpdateAndDelete(t *testing.T) {
	item := stubPutItem{
		PartitionKey: "pk",
		SortKey:      "sk",
		Title:        "title",
	}

	putArg := dynamoutil.NewPutArg(tableName, item, "attribute_not_exists(pk) AND attribute_not_exists(sk)")

	if err := dynamoutil.PutItem(context.Background(), client, putArg); err != nil {
		t.Errorf("Error putting item: %v", err)
	}

	getArg := dynamoutil.NewGetArg(tableName, dynamoutil.Keys{
		PK:     "pk",
		PKName: "pk",
		SK:     "sk",
		SKName: "sk",
	})

	result, err := dynamoutil.GetItem[stubGetItem](context.Background(), client, getArg)
	if err != nil {
		t.Errorf("Error getting item: %v", err)
	}
	if result == nil {
		t.Errorf("Item not found")
	} else if result.PartitionKey != "pk" || result.SortKey != "sk" || result.Title != "title" {
		t.Errorf("Item not matched")
	}

	updateArg := dynamoutil.NewUpdateArg(tableName, dynamoutil.Keys{
		PK:     "pk",
		PKName: "pk",
		SK:     "sk",
		SKName: "sk",
	}, stubPutItem{
		Title: "title2",
	}, "attribute_exists(pk) AND attribute_exists(sk)")

	if err := dynamoutil.UpdateItem(context.Background(), client, updateArg); err != nil {
		t.Errorf("Error updating item: %v", err)
	}

	getArg2 := dynamoutil.NewGetArg(tableName, dynamoutil.Keys{
		PK:     "pk",
		PKName: "pk",
		SK:     "sk",
		SKName: "sk",
	})

	result2, err := dynamoutil.GetItem[stubGetItem](context.Background(), client, getArg2)
	if err != nil {
		t.Errorf("Error getting item: %v", err)
	}

	if result2 == nil {
		t.Errorf("Item not found")
	} else if result2.PartitionKey != "pk" || result2.SortKey != "sk" || result2.Title != "title2" {
		t.Errorf("Item not matched")
	}

	deleteItem()
}
