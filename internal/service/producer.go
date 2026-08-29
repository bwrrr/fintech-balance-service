package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"gor/internal/domain"
	"github.com/segmentio/kafka-go"
)

type KafkaProducer struct {
	writer *kafka.Writer
}

func NewKafkaProducer(writer *kafka.Writer) *KafkaProducer {
	return &KafkaProducer{writer: writer}
}

func (p *KafkaProducer) SendTransferEvent(ctx context.Context, req domain.TransferRequest) error {
	if p.writer == nil {
		return nil
	}
	event := domain.TransferEvent{
		FromUserID: req.FromUserID,
		ToUserID:   req.ToUserID,
		Amount:     req.Amount,
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(fmt.Sprintf("%d", req.FromUserID)),
		Value: payload,
	}

	return p.writer.WriteMessages(ctx, msg)
}
