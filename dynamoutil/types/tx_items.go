package types

type (
	// context key for transaction items
	TxItemsCtxKey struct{}

	// transaction items
	TxItemsVal struct {
		TxItems []TxItem
	}

	TxItem struct {
		Method string
		PK     string
		SK     string
	}
)
