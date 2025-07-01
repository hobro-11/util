package errors

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
)

type ApiError struct {
	Code    int
	Message string
	Reason  []string
	Err     error
}

const (
	ValidationException = "ValidationException"
	ConditionalCheckFailedException = "ConditionalCheckFailedException"
)

// Error는 에러 인터페이스를 구현합니다
func (e *ApiError) Error() string {
	return e.Message
}

func (e *ApiError) Is(target error) bool {
	_, ok := target.(*ApiError)
	return ok
}

func ErrorHandle(inputErr error) error {
	var code int
	var httpErr *http.ResponseError
	if errors.As(inputErr, &httpErr) {
		code = httpErr.Response.StatusCode
	} else {
		return inputErr
	}

	var txApiErr *types.TransactionCanceledException
	if errors.As(inputErr, &txApiErr) {
		reasons := make([]string, 0)
		for _, reason := range txApiErr.CancellationReasons {
			if reason.Message == nil {
				continue
			}
			if *reason.Code == "ValidationError" {
				return &ApiError{
					Code:    code,
					Message: ValidationException,
					Reason:  []string{*reason.Message},
					Err:     txApiErr,
				}
			}
			if *reason.Code == "ConditionalCheckFailed" {
				return &ApiError{
					Code:    code,
					Message: ConditionalCheckFailedException,
					Reason:  []string{*reason.Message},
					Err:     txApiErr,
				}
			}
			reasons = append(reasons, *reason.Message)
		}
		return &ApiError{
			Code:    code,
			Message: "TransactionCanceled",
			Reason:  reasons,
			Err:     txApiErr,
		}
	}

	var condCheckFailed *types.ConditionalCheckFailedException
	if errors.As(inputErr, &condCheckFailed) {
		return &ApiError{
			Code:    code,
			Message: ConditionalCheckFailedException,
			Reason:  []string{*condCheckFailed.Message},
			Err:     condCheckFailed,
		}
	}

	var apiError smithy.APIError
	if errors.As(inputErr, &apiError) {
		if apiError.ErrorCode() == "ValidationException" {
			return &ApiError{
				Code:    code,
				Message: ValidationException,
				Reason:  []string{apiError.ErrorMessage()},
				Err:     apiError,
			}
		}
		if apiError.ErrorCode() == "ConditionalCheckFailedException" {
			return &ApiError{
				Code:    code,
				Message: ConditionalCheckFailedException,
				Reason:  []string{apiError.ErrorMessage()},
				Err:     apiError,
			}
		}
		return &ApiError{
			Code:    code,
			Message: apiError.ErrorCode(),
			Reason:  []string{apiError.ErrorMessage()},
			Err:     apiError,
		}
	}
	return inputErr
}
