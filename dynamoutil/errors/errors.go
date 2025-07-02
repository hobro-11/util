package errors

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/transport/http"
)

type ApiError struct {
	Message string
}

const (
	ValidationException             = "ValidationException"
	ValidationError                 = "ValidationError"
	ConditionalCheckFailedException = "ConditionalCheckFailedException"
	TransactionCanceledException    = "TransactionCanceledException"
	ConditionalReqFailedMessage     = "The conditional request failed"
)

var (
	ErrConditionFailed  = errors.New("condition failed")
	ErrValidationFailed = errors.New("validation failed")
	ErrInternalError    = errors.New("internal error")
	ErrUnknown          = errors.New("unknown error")
)

// Error는 에러 인터페이스를 구현합니다
func (e *ApiError) Error() string {
	return e.Message
}

func ErrorHandle(inputErr error) error {
	var (
		code     int
		httpErr  *http.ResponseError
		apiError smithy.APIError
	)

	if errors.As(inputErr, &httpErr) {
		code = httpErr.Response.StatusCode
		if code == 500 {
			code = 500
		}
	}

	if errors.As(inputErr, &apiError) {
		if code == 500 {
			return fmt.Errorf("%w: %w", ErrInternalError, apiError)
		}

		if apiError.ErrorCode() == ValidationException {
			return fmt.Errorf("%w: %w", ErrValidationFailed, apiError)
		}

		if apiError.ErrorCode() == ConditionalCheckFailedException {
			return fmt.Errorf("%w: %w", ErrConditionFailed, apiError)
		}

		var txApiErr *types.TransactionCanceledException
		if errors.As(inputErr, &txApiErr) {
			reason := txApiErr.CancellationReasons[0]
			if reason.Message != nil {
				if *reason.Message == ConditionalReqFailedMessage {
					return fmt.Errorf("%w: %w", ErrConditionFailed, txApiErr)
				}
			}

			if reason.Code != nil {
				if *reason.Code == ValidationError {
					return fmt.Errorf("%w: %w", ErrValidationFailed, txApiErr)
				}
			}

			return fmt.Errorf("%w: %w", ErrUnknown, txApiErr)
		}

		return fmt.Errorf("%w: %w", ErrUnknown, apiError)
	}

	if code == 500 {
		return fmt.Errorf("%w: %w", ErrInternalError, inputErr)
	}
	return fmt.Errorf("%w: %w", ErrUnknown, inputErr)
}
