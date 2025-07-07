package errors

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/transport/http"
)

const (
	None                            = "None"
	ValidationException             = "ValidationException"
	ValidationError                 = "ValidationError"
	ConditionalCheckFailedException = "ConditionalCheckFailedException"
	TransactionCanceledException    = "TransactionCanceledException"
	ConditionalReqFailedMessage     = "The conditional request failed"
)

func ErrorHandle(inputErr error) error {
	var (
		code     int
		httpErr  *http.ResponseError
		txApiErr *types.TransactionCanceledException
		apiError smithy.APIError
	)

	if errors.As(inputErr, &httpErr) {
		code = httpErr.Response.StatusCode
		if code == 500 {
			code = 500
		}
	}

	if errors.As(inputErr, &txApiErr) {
		for _, reason := range txApiErr.CancellationReasons {
			if reason.Message != nil {
				if *reason.Message == ConditionalReqFailedMessage {
					return &ErrConditionFailed{
						Err: txApiErr,
					}
				}
			}

			if reason.Code != nil {
				if *reason.Code == ValidationError {
					return &ErrValidationFailed{
						Err: txApiErr,
					}
				}

				if *reason.Code == ConditionalCheckFailedException {
					return &ErrConditionFailed{
						Err: txApiErr,
					}
				}

				if *reason.Code == None {
					continue
				}
			}
		}

		return &ErrOperationFailed{
			Code: code,
			Err:  txApiErr,
		}
	}

	if errors.As(inputErr, &apiError) {
		if code == 500 {
			return &ErrInternalError{
				Err: apiError,
			}
		}

		if apiError.ErrorCode() == ValidationException {
			return &ErrValidationFailed{
				Err: apiError,
			}
		}

		if apiError.ErrorCode() == ConditionalCheckFailedException {
			return &ErrConditionFailed{
				Err: apiError,
			}
		}

		return &ErrInternalError{
			Err: apiError,
		}
	}

	if code == 500 {
		return &ErrInternalError{
			Err: inputErr,
		}
	}

	return &ErrOperationFailed{
		Code: code,
		Err:  inputErr,
	}
}
