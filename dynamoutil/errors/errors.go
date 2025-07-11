package errors

import (
	"fmt"
	"strings"

	"github.com/hobro-11/util/dynamoutil/types"
)

const (
	TX_ERR_REASON_CONDITION_FAILED  = "ConditionFailed"
	TX_ERR_REASON_CONFLICT_FAILED   = "ConflictFailed"
	TX_ERR_REASON_VALIDATION_FAILED = "ValidationFailed"
	TX_ERR_NONE = "None"
)

const TX_MASSAGE_FORMAT = "Code=%s Method=%s PK=%s SK=%s"

type (
	// ApiError is the interface for all custom API errors.
	ApiError interface {
		error
		Unwrap() error
		Status() int
	}

	// ErrConditionFailed is returned when a conditional check fails.
	// This can happen in single operations (Put, Update, Delete) or within a transaction.
	ErrConditionFailed struct {
		Err error
	}

	// ErrValidationFailed is returned for a validation error, e.g. invalid request.
	ErrValidationFailed struct {
		Err error
	}

	// ErrConflict is returned when a transaction conflicts with another transaction.
	ErrConflict struct {
		Err error
	}

	// ErrInternalError is returned for an internal DynamoDB error.
	ErrInternalError struct {
		Err error
	}

	// ErrOperationFailed is a generic wrapper for other DynamoDB operation failures.
	ErrOperationFailed struct {
		HttpStatus int
		Err        error
	}

	// ErrTransactionFailed is returned when a TransactWriteItems operation fails.
	// It contains a list of reasons for the failure of each item in the transaction.
	ErrTransactionFailed struct {
		HttpStatus int
		Reasons    []TxCanceledReason
		Err        error
	}

	// TxCanceledReason holds the specific error for a single item within a failed transaction.
	TxCanceledReason struct {
		Code   string // The specific error, e.g., ErrConditionFailed. Nil if the item succeeded.
		TxItem types.TxItem
	}
)

func (e *ErrConditionFailed) Status() int {
	return 400
}

func (e *ErrConditionFailed) Error() string {
	return "condition failed"
}

func (e *ErrConditionFailed) Unwrap() error {
	return e.Err
}

func (e *ErrValidationFailed) Status() int {
	return 400
}

func (e *ErrValidationFailed) Error() string {
	return "validation failed"
}

func (e *ErrValidationFailed) Unwrap() error {
	return e.Err
}

func (e *ErrConflict) Status() int {
	return 409
}

func (e *ErrConflict) Error() string {
	return "conflicted"
}

func (e *ErrConflict) Unwrap() error {
	return e.Err
}

func (e *ErrInternalError) Status() int {
	return 500
}

func (e *ErrInternalError) Error() string {
	return "internal error"
}

func (e *ErrInternalError) Unwrap() error {
	return e.Err
}

func (e *ErrOperationFailed) Status() int {
	return e.HttpStatus
}

func (e *ErrOperationFailed) Error() string {
	return "operation failed"
}

func (e *ErrOperationFailed) Unwrap() error {
	return e.Err
}

func (e *ErrTransactionFailed) Status() int {
	return e.HttpStatus
}

func (e *ErrTransactionFailed) Error() string {
	msgs := make([]string, 0, len(e.Reasons))
	for _, reason := range e.Reasons {
		msgs = append(msgs, fmt.Sprintf(TX_MASSAGE_FORMAT, reason.Code, reason.TxItem.Method, reason.TxItem.PK, reason.TxItem.SK))
	}
	return fmt.Sprintf("transaction failed: %s", strings.Join(msgs, ", "))
}

func (e *ErrTransactionFailed) Unwrap() error {
	return e.Err
}

func (e *ErrTransactionFailed) GetReason() []TxCanceledReason {
	return e.Reasons
}
