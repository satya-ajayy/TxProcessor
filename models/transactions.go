package models

type Transaction struct {
	TxID            string  `json:"transaction_id"`
	UserID          string  `json:"user_id"`
	Amount          float32 `json:"amount"`
	Currency        string  `json:"currency"`
	TransactionType string  `json:"transaction_type"`
	Status          string  `json:"status"`
	Timestamp       string  `json:"timestamp"`
	PaymentMethod   string  `json:"payment_method"`
	CardNumber      string  `json:"card_number"`
	BankName        string  `json:"bank_name"`
	MerchantName    string  `json:"merchant_name"`
	Location        string  `json:"location"`
	Category        string  `json:"category"`
	InvoiceNumber   string  `json:"invoice_number"`
	Discount        float64 `json:"discount"`
	IPAddress       string  `json:"ip_address"`
}

type MongoTransaction struct {
	TxID            string  `json:"transaction_id" bson:"_id"`
	Amount          float32 `json:"amount" bson:"amount"`
	Currency        string  `json:"currency" bson:"currency"`
	TransactionType string  `json:"transaction_type" bson:"transaction_type"`
	Status          string  `json:"status" bson:"status"`
	Timestamp       string  `json:"timestamp" bson:"timestamp"`
	PaymentMethod   string  `json:"payment_method" bson:"payment_method"`
}

func (t *Transaction) Transform() MongoTransaction {
	return MongoTransaction{
		TxID:            t.TxID,
		Amount:          t.Amount,
		Currency:        t.Currency,
		TransactionType: t.TransactionType,
		Status:          t.Status,
		Timestamp:       t.Timestamp,
		PaymentMethod:   t.PaymentMethod,
	}
}
