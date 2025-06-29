package errors

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)
type ApiError struct{
	Code int
	Message string
}

var ErrConditionFailed = &ApiError{
	Code:    400,
	Message: "The conditional request failed",
}

// Error는 에러 인터페이스를 구현합니다
func (e *ApiError) Error() string {
	return e.Message
}

func IsConditionFailedError(inputErr error) (isFailed bool, err error) {
	var conditionalCheckFailedException *types.ConditionalCheckFailedException
	if errors.As(inputErr, &conditionalCheckFailedException) {
		return true, ErrConditionFailed
	}
	return false, nil
}
