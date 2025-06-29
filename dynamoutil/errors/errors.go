package errors

import (
	"errors"
	"fmt"

	"github.com/aws/smithy-go"
)

type APIError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Error는 에러 인터페이스를 구현합니다
func (e *APIError) Error() string {
	return e.Message
}

var ErrConditionFailed = &APIError{
	Code:    400,
	Message: "condition failed : %s",
}

func NewErrConditionFailed(msg string) error {
	return fmt.Errorf(ErrConditionFailed.Message, msg)
}

func IsConditionFailedError(err error) (isFailed bool, msg string) {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorCode() == "ConditionalCheckFailedException" {
			return true, apiErr.ErrorMessage()
		}
	}
	return false, ""
}
