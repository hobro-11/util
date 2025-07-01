package dynamoutil

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	dynamo_err "github.com/hobro-11/util/dynamoutil/errors"
)

func GetNextSequence(client *dynamodb.Client, tableName, counterId string) (uint, error) {
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName + "_sequence"),
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: counterId},
		},
		UpdateExpression: aws.String("SET #val = if_not_exists(#val, :start_val) + :inc"),
		ExpressionAttributeNames: map[string]string{
			"#val": "currentValue",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":inc":       &types.AttributeValueMemberN{Value: "1"},
			":start_val": &types.AttributeValueMemberN{Value: "0"},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	}

	result, err := client.UpdateItem(context.TODO(), input)
	if err != nil {
		return 0, fmt.Errorf("failed to get next sequence: %v", err)
	}

	currentValueAttr, ok := result.Attributes["currentValue"]
	if !ok {
		return 0, fmt.Errorf("currentValue not found in response")
	}

	currentValueN, ok := currentValueAttr.(*types.AttributeValueMemberN)
	if !ok {
		return 0, fmt.Errorf("currentValue is not a number")
	}

	seq, err := strconv.ParseInt(currentValueN.Value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse sequence number: %v", err)
	}

	return uint(seq), nil
}

func GetItem[Dest any](ctx context.Context, client *dynamodb.Client, getArg *GetArg) (*Dest, error) {
	input := dynamodb.GetItemInput{}
	input.TableName = getArg.getTableName()
	input.Key = getArg.getKey()
	projectionExp, err := GenerateProjectionExpression[Dest]()
	if err != nil {
		return nil, err
	}
	input.ProjectionExpression = aws.String(projectionExp)

	result, err := client.GetItem(ctx, &input)

	if err != nil {
		return nil, err
	}

	dest := new(Dest)
	err = attributevalue.UnmarshalMap(result.Item, dest)
	if err != nil {
		return nil, err
	}

	return dest, nil
}

// GenerateProjectionExpression은 제네릭 타입의 구조체를 분석하여 프로젝션 표현식을 생성합니다.
// 예: "Title, Email, Address.City"
func GenerateProjectionExpression[T any]() (string, error) {
	var t T
	tType := reflect.TypeOf(t)

	// 포인터 타입인 경우 요소 타입을 가져옵니다.
	if tType.Kind() == reflect.Pointer {
		tType = tType.Elem()
	}

	// 구조체가 아닌 경우 오류 반환
	if tType.Kind() != reflect.Struct {
		return "", fmt.Errorf("expected a struct type, got %v", tType.Kind())
	}

	var fields []string
	for i := 0; i < tType.NumField(); i++ {
		field := tType.Field(i)

		// dynamodbav 태그 확인
		tag := field.Tag.Get("dynamodbav")
		if tag == "" {
			// 태그가 없는 경우 필드 이름을 소문자로 사용
			fields = append(fields, field.Name)
			continue
		}

		// 태그에서 옵션 제거 (예: `dynamodbav:"name,omitempty"` -> "name")
		parts := strings.Split(tag, ",")
		if parts[0] != "" && parts[0] != "-" {
			fields = append(fields, parts[0])
		}
	}

	if len(fields) == 0 {
		return "", fmt.Errorf("no fields found with dynamodbav tags")
	}

	return strings.Join(fields, ", "), nil
}

func PutItem(ctx context.Context, client *dynamodb.Client, putArg *PutArg) error {
	input := dynamodb.PutItemInput{}
	input.TableName = putArg.getTableName()
	input.Item = putArg.getItem()
	if putArg.getConditionExp() != nil {
		input.ConditionExpression = putArg.getConditionExp()
	}

	_, err := client.PutItem(ctx, &input)
	if err != nil {
		return dynamo_err.ErrorHandle(err)
	}

	return nil
}

// updateArg can't be nil
// if occur conditionCheckFailed, return errors.ErrConditionFailed
func UpdateItem(ctx context.Context, client *dynamodb.Client, updateArg *UpdateArg) error {
	updateExp, expAttNames, expAttValues := GetUpdateProps(updateArg.getItem())
	input := dynamodb.UpdateItemInput{}
	input.TableName = updateArg.getTableName()
	input.Key = updateArg.getKey()
	input.UpdateExpression = aws.String(updateExp)
	input.ExpressionAttributeNames = expAttNames
	input.ExpressionAttributeValues = expAttValues
	if updateArg.getConditionExp() != nil {
		input.ConditionExpression = updateArg.getConditionExp()
	}

	_, err := client.UpdateItem(ctx, &input)
	if err != nil {
		return dynamo_err.ErrorHandle(err)
	}

	return nil
}

// deleteArg can't be nil
// if occur conditionCheckFailed, return errors.ErrConditionFailed
func DeleteItem(ctx context.Context, client *dynamodb.Client, deleteArg *DeleteArg) error {
	input := dynamodb.DeleteItemInput{}
	input.TableName = deleteArg.getTableName()
	input.Key = deleteArg.getKey()
	if deleteArg.getConditionExp() != nil {
		input.ConditionExpression = deleteArg.getConditionExp()
	}

	_, err := client.DeleteItem(ctx, &input)

	if err != nil {
		return dynamo_err.ErrorHandle(err)
	}

	return nil
}

type WriteArg struct {
	PutArgs    []*PutArg
	UpdateArgs []*UpdateArg
	DeleteArgs []*DeleteArg
}

func TransactionWrite(ctx context.Context, client *dynamodb.Client, writeArg *WriteArg) error {
	input := make([]types.TransactWriteItem, 0, len(writeArg.PutArgs)+len(writeArg.UpdateArgs)+len(writeArg.DeleteArgs))

	for _, putArg := range writeArg.PutArgs {
		input = append(input, types.TransactWriteItem{
			Put: &types.Put{
				TableName:           putArg.getTableName(),
				Item:                putArg.getItem(),
				ConditionExpression: putArg.getConditionExp(),
			},
		})
	}

	for _, updateArg := range writeArg.UpdateArgs {
		updateExp, expAttNames, expAttValues := GetUpdateProps(updateArg.getItem())
		input = append(input, types.TransactWriteItem{
			Update: &types.Update{
				TableName:                 updateArg.getTableName(),
				Key:                       updateArg.getKey(),
				UpdateExpression:          aws.String(updateExp),
				ExpressionAttributeNames:  expAttNames,
				ExpressionAttributeValues: expAttValues,
				ConditionExpression:       updateArg.getConditionExp(),
			},
		})
	}

	for _, deleteArg := range writeArg.DeleteArgs {
		input = append(input, types.TransactWriteItem{
			Delete: &types.Delete{
				TableName:           deleteArg.getTableName(),
				Key:                 deleteArg.getKey(),
				ConditionExpression: deleteArg.getConditionExp(),
			},
		})
	}

	_, err := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: input,
	})

	if err != nil {
		return dynamo_err.ErrorHandle(err)
	}

	return nil
}
