package service

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	paymentv1 "github.com/PinkPig16/msGo/shared/pkg/proto/payment/v1"
)

// server реализует gRPC сервис оплаты
type server struct {
	paymentv1.UnimplementedPaymentServiceServer
}

// NewServer создаёт новый экземпляр сервера оплаты
func NewServer() *server {
	return &server{}
}

// PayOrder обрабатывает оплату заказа
func (s *server) PayOrder(
	ctx context.Context,
	req *paymentv1.PayOrderRequest,
) (*paymentv1.PayOrderResponse, error) {
	// 1. Проверить, что order_uuid не пустой → INVALID_ARGUMENT
	// 2. Проверить, что payment_method != UNSPECIFIED → INVALID_ARGUMENT
	// 3. Проверить формат UUID → INVALID_ARGUMENT
	// 4. Сгенерировать transaction_uuid (UUID v4)
	// 5. Вывести в лог: "оплата прошла успешно, order_uuid: X, transaction_uuid: Y"
	// 6. Вернуть transaction_uuid

	if req.GetOrderUuid() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "order_uuid:%s не должен быть пустым UU", req.GetOrderUuid())
	}
	if req.GetPaymentMethod() == paymentv1.PaymentMethod_PAYMENT_METHOD_UNSPECIFIED {
		return nil, status.Errorf(codes.InvalidArgument, "Не указан метод оплаты")
	}
	_, err := uuid.Parse(req.GetOrderUuid())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Некоррекстный формат uuid: %s", req.GetOrderUuid())
	}

	transactionUUID := uuid.New().String()

	slog.Info("оплата прошла успешно", "order_uuid, ", req.GetOrderUuid(), "transaction_uuid", transactionUUID)

	return &paymentv1.PayOrderResponse{TransactionUuid: transactionUUID}, nil
}
