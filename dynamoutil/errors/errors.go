package errors

type (
	ApiError interface {
		error
		Unwrap() error
		Status() int
	}

	ErrConditionFailed struct {
		Err error
	}

	ErrValidationFailed struct {
		Err error
	}

	ErrInternalError struct {
		Err error
	}

	ErrOperationFailed struct {
		HttpStatus int
		Err        error
	}

	ErrTransactionFailed struct {
		HttpStatus int
		Reasons    []TxCanceledReason
		Err        error
	}

	TxCanceledReason struct {
		Code   string
		TxItem TxItem
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
	return "transaction failed"
}

func (e *ErrTransactionFailed) Unwrap() error {
	return e.Err
}

func (e *ErrTransactionFailed) GetReason() []TxCanceledReason {
	return e.Reasons
}
