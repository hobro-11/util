package errors

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/transport/http"
)

const (
	None                            = "None"
	ValidationException             = "ValidationException"
	ValidationError                 = "ValidationError"
	TransactionConflictException    = "TransactionConflictException"
	ConditionalCheckFailedException = "ConditionalCheckFailedException"
	TransactionCanceledException    = "TransactionCanceledException"
	ConditionalReqFailedMessage     = "The conditional request failed"
)

type (
	TxSeqCtxKey struct{}

	TxItemSeqVal struct {
		TxItems []TxItem
	}

	TxItem struct {
		Method string
		PK     string
		SK     string
	}
)

func ErrorHandle(ctx context.Context, inputErr error) error {
	var (
		hs       int
		httpErr  *http.ResponseError
		txApiErr *types.TransactionCanceledException
		apiError smithy.APIError
		txSeqVal *TxItemSeqVal
	)

	if errors.As(inputErr, &httpErr) {
		hs = httpErr.Response.StatusCode
		if hs == 500 {
			hs = 500
		}
	}

	if errors.As(inputErr, &txApiErr) {
		txSeqVal = getTxSeqVal(ctx)

		if txSeqVal != nil {
			txErr := getTxErrAppliedTxCancelReason(txApiErr, txSeqVal)
			txErr.HttpStatus = hs
			txErr.Err = txApiErr
			return txErr
		}

		return &ErrTransactionFailed{
			HttpStatus: hs,
			Err:        txApiErr,
		}
	}

	if errors.As(inputErr, &apiError) {
		if hs == 500 {
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

	if hs == 500 {
		return &ErrInternalError{
			Err: inputErr,
		}
	}

	return &ErrOperationFailed{
		HttpStatus: hs,
		Err:        inputErr,
	}
}

func getTxSeqVal(ctx context.Context) *TxItemSeqVal {
	if txSeqVal, ok := ctx.Value(TxSeqCtxKey{}).(*TxItemSeqVal); ok {
		return txSeqVal
	}

	return nil
}

func getTxErrAppliedTxCancelReason(tae *types.TransactionCanceledException, tsv *TxItemSeqVal) *ErrTransactionFailed {
	txErr := &ErrTransactionFailed{
		Reasons: make([]TxCanceledReason, len(tae.CancellationReasons)),
	}

	for i, reason := range tae.CancellationReasons {
		if reason.Code != nil {
			if *reason.Code == "" {
				txErr.Reasons[i] = TxCanceledReason{
					Code:   None,
					TxItem: tsv.TxItems[i],
				}
			} else {
				txErr.Reasons[i] = TxCanceledReason{
					Code:   *reason.Code,
					TxItem: tsv.TxItems[i],
				}
			}
		}
	}

	return txErr
}

