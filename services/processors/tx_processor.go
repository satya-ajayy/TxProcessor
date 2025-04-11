package processors

import (
	// Go Internal Packages
	"context"
	"encoding/json"
	"fmt"

	// Local Packages
	models "tx-stream/models"

	// External Packages
	"go.uber.org/zap"
)

type TxRepository interface {
	InsertTransactions(ctx context.Context, txs []interface{}) error
	InsertTransaction(ctx context.Context, tx models.MongoTransaction) error
}

type TxProcessor struct {
	Logger *zap.Logger
	TxRepo TxRepository
}

func NewTxProcessor(logger *zap.Logger, txRepo TxRepository) *TxProcessor {
	return &TxProcessor{TxRepo: txRepo, Logger: logger}
}

func (p *TxProcessor) ProcessRecords(ctx context.Context, records []models.Record) error {
	if len(records) == 0 {
		return nil
	}
	
	var txs []interface{}
	for _, record := range records {
		var tx models.Transaction
		err := json.Unmarshal(record.Value, &tx)
		if err != nil {
			p.Logger.Error("failed to unmarshal transaction", zap.Error(err))
			continue
		}
		txs = append(txs, tx.Transform())
	}

	err := p.TxRepo.InsertTransactions(ctx, txs)
	if err != nil {
		return fmt.Errorf("failed to insert transactions: %v", err)
	}
	return nil
}

func (p *TxProcessor) ProcessRecord(ctx context.Context, record models.Record) error {
	var tx models.Transaction

	err := json.Unmarshal(record.Value, &tx)
	if err != nil {
		p.Logger.Error("failed to unmarshal transaction", zap.Error(err))
		return nil
	}

	err = p.TxRepo.InsertTransaction(ctx, tx.Transform())
	if err != nil {
		return fmt.Errorf("failed to insert transaction: %v", err)
	}
	return nil
}
