package dynamoutil

import (
	"log"
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type GetArg struct {
	TableName string
	Key       *Keys
}

func (g *GetArg) getTableName() *string {
	return aws.String(g.TableName)
}

func (g *GetArg) getKey() map[string]types.AttributeValue {
	key := make(map[string]types.AttributeValue)

	pk := MustMarshalKey(g.Key.PK)
	if pk == nil {
		return nil
	}
	key[g.Key.PKName] = pk

	sk := MustMarshalKey(g.Key.SK)
	if sk == nil {
		return key
	}
	key[g.Key.SKName] = sk

	return key
}

type UpsertArg struct {
	TableName    string
	Key          *Keys
	// item 은 구조체만 가능하다.
	// 텅 빈 속성에 대해선 update 를 진행하지 않는다.
	Item         any
	ConditionExp string
}

func (p *UpsertArg) getTableName() *string {
	return aws.String(p.TableName)
}

func (p *UpsertArg) getKey() map[string]types.AttributeValue {
	key := make(map[string]types.AttributeValue)

	pk := MustMarshalKey(p.Key.PK)
	if pk == nil {
		return nil
	}
	key[p.Key.PKName] = pk

	sk := MustMarshalKey(p.Key.SK)
	if sk == nil {
		return key
	}
	key[p.Key.SKName] = sk

	return key
}

func (p *UpsertArg) getItem() any {
	return p.Item
}

func (p *UpsertArg) getConditionExp() *string {
	if p.ConditionExp == "" {
		return nil
	}
	return aws.String(p.ConditionExp)
}

type DeleteArg struct {
	TableName    string
	Key          *Keys
	ConditionExp string
}

func (p *DeleteArg) getTableName() *string {
	return aws.String(p.TableName)
}

func (p *DeleteArg) getKey() map[string]types.AttributeValue {
	var key map[string]types.AttributeValue
	pk := MustMarshalKey(p.Key.PK)
	if p.Key.SK != nil && p.Key.SKName != "" {
		sk := MustMarshalKey(p.Key.SK)
		if sk != nil {
			key = map[string]types.AttributeValue{
				p.Key.PKName: pk,
				p.Key.SKName: sk,
			}
		}
	}
	return key
}

func (p *DeleteArg) getConditionExp() *string {
	if p.ConditionExp == "" {
		return nil
	}
	return aws.String(p.ConditionExp)
}

type QueryArg struct {
	TableName              string
	KeyConditionExpression string
	Keys                   *PkAndSkPrefix
	CursorPaging           *CursorPaging
}

type CursorPaging struct {
	IsDesc            bool
	Size              int32
	ExclusiveStartKey *Keys
}

type PkAndSkPrefix struct {
	PK       any
	SKPrefix any
	PKName   string
	SKName   string
}

func (q *QueryArg) getTableName() *string {
	return aws.String(q.TableName)
}

func (q *QueryArg) getKeyConditionExpression() *string {
	return aws.String(q.KeyConditionExpression)
}

func (q *QueryArg) getExpAttVal() map[string]types.AttributeValue {
	key := make(map[string]types.AttributeValue)

	pk := MustMarshalKey(q.Keys.PK)
	if pk == nil {
		return nil
	}
	key[":"+q.Keys.PKName] = pk

	sk := MustMarshalKey(q.Keys.SKPrefix)
	if sk == nil {
		return key
	}
	key[":"+q.Keys.SKName] = sk

	return key
}

func (q *QueryArg) IsPagination() bool {
	return q.CursorPaging != nil
}

func (q *QueryArg) getScanIndexForward() *bool {
	if q.CursorPaging.IsDesc {
		return aws.Bool(false)
	}
	return aws.Bool(true)
}

func (q *QueryArg) getExclusiveStartKey() map[string]types.AttributeValue {
	key := make(map[string]types.AttributeValue)
	if q.CursorPaging.ExclusiveStartKey == nil {
		return nil
	}

	pk := MustMarshalKey(q.CursorPaging.ExclusiveStartKey.PK)
	if pk == nil {
		return nil
	}
	key[q.CursorPaging.ExclusiveStartKey.PKName] = pk

	if q.CursorPaging.ExclusiveStartKey.SK == nil {
		return key
	}

	sk := MustMarshalKey(q.CursorPaging.ExclusiveStartKey.SK)
	if sk == nil {
		return key
	}
	key[q.CursorPaging.ExclusiveStartKey.SKName] = sk

	return key
}

func (q *QueryArg) getLimit() int32 {
	if q.CursorPaging.Size == 0 {
		q.CursorPaging.Size = 10
	}
	return q.CursorPaging.Size
}

func NewBatchGetArg(tableName string, keys PkAndSks) *BatchGetArg {
	return &BatchGetArg{
		TableName: tableName,
		PkAndSks:  &keys,
	}
}

type BatchGetArg struct {
	TableName string
	PkAndSks  *PkAndSks
}

type PkAndSks struct {
	PK     any
	SKs    []any
	PKName string
	SKName string
}

func (b *BatchGetArg) getTableName() string {
	return b.TableName
}

func (b *BatchGetArg) getPkAndSks() PkAndSks {
	return *b.PkAndSks
}

type Keys struct {
	// PK 값은 필수이다.
	PK     any
	PKName string
	SK     any
	SKName string
}

// number와 string 만 지원, 지원하지 않는 타입의 경우 nil을 반환한다.
// if key is nil, return nil
func MustMarshalKey(key any) types.AttributeValue {
	if key == nil {
		return nil
	}
	switch v := key.(type) {
	case string:
		return &types.AttributeValueMemberS{Value: v}
	case int64:
		return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
	case int32:
		return &types.AttributeValueMemberN{Value: strconv.Itoa(int(v))}
	case int16:
		return &types.AttributeValueMemberN{Value: strconv.Itoa(int(v))}
	case int:
		return &types.AttributeValueMemberN{Value: strconv.Itoa(v)}
	case uint64:
		return &types.AttributeValueMemberN{Value: strconv.FormatUint(v, 10)}
	case uint:
		return &types.AttributeValueMemberN{Value: strconv.FormatUint(uint64(v), 10)}
	case uint16:
		return &types.AttributeValueMemberN{Value: strconv.FormatUint(uint64(v), 10)}
	case uint8:
		return &types.AttributeValueMemberN{Value: strconv.FormatUint(uint64(v), 10)}
	case uint32:
		return &types.AttributeValueMemberN{Value: strconv.FormatUint(uint64(v), 10)}
	default:
		return nil
	}
}

// Helper function to marshal Go types to DynamoDB attribute values
func MustMarshalItem(in interface{}) map[string]types.AttributeValue {
	av, err := attributevalue.MarshalMap(in)
	if err != nil {
		log.Fatalf("Failed to marshal item: %v", err)
	}
	return av
}

func GetUpdateProps(input any) (updateExp string, expAttNames map[string]string, expAttValues map[string]types.AttributeValue) {
	var setExpressions []string
	expAttNames = make(map[string]string)
	expAttValues = make(map[string]types.AttributeValue)
	nameCounter := 0

	val := reflect.ValueOf(input)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return "", nil, nil
	}

	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)
		tag := fieldType.Tag.Get("dynamodbav")

		// The tag can be "my-field,omitempty". We only need "my-field".
		tagParts := strings.Split(tag, ",")
		columnName := tagParts[0]

		if columnName == "-" {
			continue
		}

		// Skip zero values, which is a simple way to emulate `omitempty` for update expressions.
		if !field.IsValid() || reflect.DeepEqual(field.Interface(), reflect.Zero(field.Type()).Interface()) {
			continue
		}

		// If the tag is present but the name is empty, fall back to the struct field name.
		if columnName == "" {
			columnName = fieldType.Name
		}

		nameKey := "#f" + strconv.Itoa(nameCounter)
		valueKey := ":v" + strconv.Itoa(nameCounter)
		nameCounter++

		expAttNames[nameKey] = columnName

		// Marshal the field's value into a DynamoDB attribute value.
		// This correctly handles all supported data types (string, number, bool, list, map, etc.).
		av, err := attributevalue.Marshal(field.Interface())
		if err != nil {
			// Consider logging this error for debugging purposes.
			continue
		}

		// If marshaling results in a null attribute value (e.g. for a nil pointer), skip it.
		if _, ok := av.(*types.AttributeValueMemberNULL); ok {
			continue
		}

		expAttValues[valueKey] = av
		setExpressions = append(setExpressions, nameKey+" = "+valueKey)
	}

	if len(setExpressions) == 0 {
		return "", nil, nil
	}

	updateExp = "SET " + strings.Join(setExpressions, ", ")
	return updateExp, expAttNames, expAttValues
}
