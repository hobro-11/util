package dynamo_util

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func QueryGetItems[Dest any](ctx context.Context, client *dynamodb.Client, arg *QueryArg) ([]Dest, error) {
	input := dynamodb.QueryInput{}
	input.TableName = arg.getTableName()
	input.KeyConditionExpression = arg.getKeyConditionExpression()
	input.ExpressionAttributeValues = arg.getExpAttVal()

	if arg.IsPagination() {
		input.ScanIndexForward = arg.getScanIndexForward()
		input.Limit = aws.Int32(int32(arg.getLimit()))
		input.ExclusiveStartKey = arg.getExclusiveStartKey()
	}

	projectionExp, err := GenerateProjectionExpression[Dest]()
	if err != nil {
		return nil, err
	}

	input.ProjectionExpression = aws.String(projectionExp)

	result, err := client.Query(ctx, &input)

	if err != nil {
		return nil, err
	}

	if len(result.Items) == 0 {
		return nil, nil
	}

	dest := make([]Dest, 0, len(result.Items))
	for _, item := range result.Items {
		var temp Dest
		err = attributevalue.UnmarshalMap(item, &temp)
		if err != nil {
			return nil, err
		}
		dest = append(dest, temp)
	}

	return dest, nil
}

func BatchGetItems[Dest any](ctx context.Context, client *dynamodb.Client, arg *BatchGetArg) ([]Dest, error) {
	projectionExp, err := GenerateProjectionExpression[Dest]()
	if err != nil {
		return nil, err
	}

	keys := arg.getPkAndSks()

	k := make([]map[string]types.AttributeValue, 0, len(keys.SKs))
	for _, v := range keys.SKs {
		k = append(k, map[string]types.AttributeValue{
			keys.PKName: MustMarshalKey(keys.PK),
			keys.SKName: MustMarshalKey(v),
		})
	}

	r, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			arg.getTableName(): {
				Keys:                 k,
				ProjectionExpression: aws.String(projectionExp),
			},
		},
	})

	if err != nil {
		return nil, err
	}

	if len(r.Responses) == 0 {
		return nil, nil
	}

	if len(r.Responses[arg.getTableName()]) == 0 {
		return nil, nil
	}

	result := make([]Dest, 0, len(r.Responses[arg.getTableName()]))
	for _, item := range r.Responses[arg.getTableName()] {
		var temp Dest
		err = attributevalue.UnmarshalMap(item, &temp)
		if err != nil {
			return nil, err
		}
		result = append(result, temp)
	}

	return result, nil
}
