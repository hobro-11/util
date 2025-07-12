package errors

import (
	"context"
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/transport/http"
	api_types "github.com/hobro-11/util/dynamoutil/types"
)

const (
	CONDITION  = "condition"
	CONFLICT   = "conflict"
	VALIDATION = "validation"
	NONE       = "none"
)

func ErrorHandle(ctx context.Context, inputErr error) error {
	var httpStatus int
	var httpErr *http.ResponseError
	if errors.As(inputErr, &httpErr) {
		httpStatus = httpErr.Response.StatusCode
	}

	var txApiErr *types.TransactionCanceledException
	if errors.As(inputErr, &txApiErr) {
		return getTxErrAppliedTxCancelReason(ctx, httpStatus, txApiErr)
	}

	var apiError smithy.APIError
	if errors.As(inputErr, &apiError) {
		if httpStatus >= 500 {
			return &ErrInternalError{
				Err: apiError,
			}
		}

		code := strings.ToLower(apiError.ErrorCode())

		if contains(code, CONDITION) {
			return &ErrConditionFailed{
				Err: apiError,
			}
		} else if contains(code, CONFLICT) {
			return &ErrConflict{
				Err: apiError,
			}
		} else if contains(code, VALIDATION) {
			return &ErrValidationFailed{
				Err: apiError,
			}
		} else {
			return &ErrOperationFailed{
				HttpStatus: httpStatus,
				Err:        apiError,
			}
		}
	}

	if httpStatus >= 500 {
		return &ErrInternalError{
			Err: inputErr,
		}
	}

	return &ErrOperationFailed{
		HttpStatus: httpStatus,
		Err:        inputErr,
	}
}

func getTxSeqVal(ctx context.Context) *api_types.TxItemsVal {
	if txSeqVal, ok := ctx.Value(api_types.TxItemsCtxKey{}).(*api_types.TxItemsVal); ok {
		return txSeqVal
	}
	return nil
}

func getTxErrAppliedTxCancelReason(ctx context.Context, hs int, tae *types.TransactionCanceledException) *ErrTransactionFailed {
	txErr := &ErrTransactionFailed{
		HttpStatus: hs,
		Err:        tae,
	}

	txSeqVal := getTxSeqVal(ctx)
	if txSeqVal == nil {
		return txErr
	}

	errReasons := make([]TxCanceledReason, len(tae.CancellationReasons))

	for i, reason := range tae.CancellationReasons {
		var code string
		if reason.Code != nil {
			tempCode := strings.ToLower(*reason.Code)

			if contains(tempCode, CONDITION) {
				code = TX_ERR_REASON_CONDITION_FAILED
			} else if contains(tempCode, CONFLICT) {
				code = TX_ERR_REASON_CONFLICT_FAILED
			} else if contains(tempCode, VALIDATION) {
				code = TX_ERR_REASON_VALIDATION_FAILED
			} else if contains(tempCode, NONE) {
				code = TX_ERR_NONE
			}

			errReasons[i] = TxCanceledReason{
				Code:   code,
				TxItem: txSeqVal.TxItems[i],
			}
		}
	}

	txErr.Reasons = errReasons
	txErr.Err = tae
	txErr.HttpStatus = hs
	return txErr
}

func contains(errCode string, target string) bool {
	return strings.Contains(errCode, target)
}
