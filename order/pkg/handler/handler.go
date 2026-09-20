package handler

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	orderv1 "github.com/PinkPig16/msGo/shared/pkg/openapi/order/v1"
	inventoryv1 "github.com/PinkPig16/msGo/shared/pkg/proto/inventory/v1"
	paymentv1 "github.com/PinkPig16/msGo/shared/pkg/proto/payment/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// OrderStatus — статус заказа
type OrderStatus string

const (
	OrderStatusPendingPayment OrderStatus = "PENDING_PAYMENT"
	OrderStatusPaid           OrderStatus = "PAID"
	OrderStatusCancelled      OrderStatus = "CANCELLED"
)

// PaymentMethod — способ оплаты заказа
type PaymentMethod string

const (
	PaymentMethodCard          PaymentMethod = "CARD"
	PaymentMethodSBP           PaymentMethod = "SBP"
	PaymentMethodCreditCard    PaymentMethod = "CREDIT_CARD"
	PaymentMethodInvestorMoney PaymentMethod = "INVESTOR_MONEY"
)

var ErrPartUnavailableStock = errors.New("недостаточно кол-во деталей UUID")

// Order представляет заказ на постройку космического корабля
type Order struct {
	OrderUUID       uuid.UUID
	HullUUID        uuid.UUID
	EngineUUID      uuid.UUID
	ShieldUUID      *uuid.UUID // опциональный
	WeaponUUID      *uuid.UUID // опциональный
	TotalPrice      int64      // в копейках
	TransactionUUID *uuid.UUID
	PaymentMethod   *PaymentMethod
	Status          OrderStatus
	CreatedAt       time.Time
}

// orderStore — хранилище заказов (in-memory)
type orderStore struct {
	mu     sync.RWMutex
	orders map[uuid.UUID]Order
}

// NewOrderStore создаёт новое пустое хранилище заказов
func NewOrderStore() *orderStore {
	return &orderStore{
		orders: make(map[uuid.UUID]Order),
	}
}

// handler реализует интерфейс orderv1.Handler, сгенерированный ogen
type handler struct {
	orderv1.UnimplementedHandler
	inventoryClient inventoryv1.InventoryServiceClient
	paymentClient   paymentv1.PaymentServiceClient
	store           *orderStore
}

// NewHandler создаёт новый обработчик заказов
func NewHandler(
	inventoryClient inventoryv1.InventoryServiceClient,
	paymentClient paymentv1.PaymentServiceClient,
	store *orderStore,
) *handler {
	return &handler{
		inventoryClient: inventoryClient,
		paymentClient:   paymentClient,
		store:           store,
	}
}

// SetupServer создаёт OpenAPI сервер на основе обработчика
func SetupServer(h *handler) (*orderv1.Server, error) {
	return orderv1.NewServer(h)
}

// GetOrder реализует операцию getOrder (пример реализации)
// GET /api/v1/orders/{order_uuid}.
func (h *handler) GetOrder(_ context.Context, params orderv1.GetOrderParams) (orderv1.GetOrderRes, error) {
	// 1. Найти заказ в store (с блокировкой для thread-safety)
	h.store.mu.RLock()
	order, ok := h.store.orders[params.OrderUUID]
	h.store.mu.RUnlock()

	// 2. Если не найден — вернуть 404
	if !ok {
		return &orderv1.GetOrderNotFound{
			Code:    http.StatusNotFound,
			Message: "заказ не найден",
		}, nil
	}

	// 3. Преобразовать в DTO и вернуть
	var shieldUUID orderv1.OptNilUUID
	if order.ShieldUUID != nil {
		shieldUUID = orderv1.NewOptNilUUID(*order.ShieldUUID)
	}

	var weaponUUID orderv1.OptNilUUID
	if order.WeaponUUID != nil {
		weaponUUID = orderv1.NewOptNilUUID(*order.WeaponUUID)
	}

	var transactionUUID orderv1.OptNilUUID
	if order.TransactionUUID != nil {
		transactionUUID = orderv1.NewOptNilUUID(*order.TransactionUUID)
	}

	var paymentMethod orderv1.OptNilPaymentMethod
	if order.PaymentMethod != nil {
		paymentMethod = orderv1.NewOptNilPaymentMethod(orderv1.PaymentMethod(*order.PaymentMethod))
	}

	return &orderv1.OrderDto{
		OrderUUID:       order.OrderUUID,
		HullUUID:        order.HullUUID,
		EngineUUID:      order.EngineUUID,
		ShieldUUID:      shieldUUID,
		WeaponUUID:      weaponUUID,
		TotalPrice:      order.TotalPrice,
		TransactionUUID: transactionUUID,
		PaymentMethod:   paymentMethod,
		Status:          orderv1.OrderStatus(order.Status),
		CreatedAt:       order.CreatedAt,
	}, nil
}

// CreateOrder orderv1.Handler:
func (h *handler) CreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest) (orderv1.CreateOrderRes, error) {
	// 1. Валидация: hull_uuid и engine_uuid обязательны
	// 2. Получить детали через InventoryService.ListParts
	// 3. Проверить stock_quantity > 0
	// 4. Вычислить total_price
	// 5. Сгенерировать order_uuid (UUID v4)
	// 6. Создать заказ со статусом PENDING_PAYMENT
	// 7. Сохранить в store
	// 8. Вернуть order_uuid и total_price

	uuidSlice := make([]string, 0, 5)

	uuidSlice = append(uuidSlice, req.GetHullUUID().String())
	uuidSlice = append(uuidSlice, req.GetEngineUUID().String())

	if req.GetShieldUUID().Set {
		uuidSlice = append(uuidSlice, req.GetShieldUUID().Value.String())
	}

	if req.GetWeaponUUID().Set {
		uuidSlice = append(uuidSlice, req.GetWeaponUUID().Value.String())
	}
	slog.Info("Get parts from inventoryServer")
	partsResponse, err := h.inventoryClient.ListParts(ctx, &inventoryv1.ListPartsRequest{Uuids: uuidSlice})

	if err != nil {
		slog.Error("Get parts from inventoryServer", "err", err)
		switch status.Code(err) {
		case codes.InvalidArgument:
			return CreateOrderBadRequest(), nil
		case codes.NotFound:
			return CreateOrderNotFound(), nil
		default:
			return CreateOrderInternalServerError(), nil
		}
	}

	availableParts, err := AddAvailableParts(partsResponse.Parts)
	if err != nil {
		switch {
		case errors.Is(err, ErrPartUnavailableStock):
			return CreateOrderConflict(), nil
		case err != nil:
			return CreateOrderInternalServerError(), nil
		}
	}

	order := &Order{
		OrderUUID:       uuid.New(),
		EngineUUID:      req.GetEngineUUID(),
		HullUUID:        req.GetHullUUID(),
		ShieldUUID:      getPartUUIdByPartType(inventoryv1.PartType_PART_TYPE_SHIELD, availableParts),
		WeaponUUID:      getPartUUIdByPartType(inventoryv1.PartType_PART_TYPE_WEAPON, availableParts),
		TotalPrice:      TotalPartsPrice(availableParts),
		TransactionUUID: nil,
		PaymentMethod:   nil,
		Status:          OrderStatusPendingPayment,
		CreatedAt:       time.Now(),
	}

	h.store.mu.Lock()
	h.store.orders[order.OrderUUID] = *order
	h.store.mu.Unlock()
	slog.Info("Order created", "uuid", order.OrderUUID)
	return &orderv1.CreateOrderResponse{
		OrderUUID:  order.OrderUUID,
		TotalPrice: order.TotalPrice,
	}, nil
}

func CreateOrderConflict() orderv1.CreateOrderRes {
	return &orderv1.CreateOrderConflict{
		Code:    409,
		Message: "Недостаточно материала",
	}
}

func CreateOrderNotFound() orderv1.CreateOrderRes {
	return &orderv1.CreateOrderNotFound{
		Code:    404,
		Message: "Материал не найден",
	}
}

func CreateOrderBadRequest() orderv1.CreateOrderRes {
	return &orderv1.CreateOrderBadRequest{
		Code:    400,
		Message: "Невозможно создать заказ так-как не доступных материалов",
	}
}

func CreateOrderInternalServerError() orderv1.CreateOrderRes {
	return &orderv1.CreateOrderInternalServerError{
		Code:    500,
		Message: "Внутрення ошибка сервера",
	}
}
func PayOrderNotFound() orderv1.PayOrderRes {
	return &orderv1.PayOrderNotFound{
		Code:    404,
		Message: "Заказ не найден",
	}
}

func PayOrderConflict(message string) orderv1.PayOrderRes {
	return &orderv1.PayOrderConflict{
		Code:    409,
		Message: message,
	}
}

func PayOrderBadRequest() orderv1.PayOrderRes {
	return &orderv1.PayOrderBadRequest{
		Code:    400,
		Message: "Некорретно переданные данные",
	}
}
func PayOrderInternalServerError() orderv1.PayOrderRes {
	return &orderv1.PayOrderInternalServerError{
		Code:    500,
		Message: "Некорекстная работа сервера",
	}
}

func AddAvailableParts(parts []*inventoryv1.Part) ([]*inventoryv1.Part, error) {
	availableParts := make([]*inventoryv1.Part, 0, len(parts))
	for _, part := range parts {
		if part.StockQuantity <= 0 {
			return nil, ErrPartUnavailableStock
		}
		availableParts = append(availableParts, part)
	}
	return availableParts, nil
}

func getPartUUIdByPartType(partType inventoryv1.PartType, parts []*inventoryv1.Part) *uuid.UUID {
	for _, part := range parts {
		if part.PartType == partType {
			res, err := uuid.Parse(part.Uuid)
			if err == nil {
				return &res
			}
		}
	}
	return nil
}

func TotalPartsPrice(parts []*inventoryv1.Part) int64 {
	sum := int64(0)
	for _, part := range parts {
		sum += part.Price
	}
	return sum
}

// PayOrder реализует операцию payOrder
// POST /api/v1/orders/{order_uuid}/pay
func (h *handler) PayOrder(ctx context.Context, req *orderv1.PayOrderRequest, params orderv1.PayOrderParams) (orderv1.PayOrderRes, error) {
	// 1. Найти заказ в store
	// 2. Проверить статус == PENDING_PAYMENT
	// 3. Вызвать h.paymentClient.PayOrder для обработки платежа
	// 4. Обновить статус на PAID и сохранить transaction_uuid
	// 5. Вернуть transaction_uuid

	h.store.mu.RLock()
	order, ok := h.store.orders[params.OrderUUID]
	h.store.mu.RUnlock()
	if !ok {
		return PayOrderNotFound(), nil
	}

	if order.Status != OrderStatusPendingPayment {
		switch order.Status {
		case OrderStatusPaid:
			return PayOrderConflict("Заказ уже оплачен"), nil
		case OrderStatusCancelled:
			return PayOrderConflict("Заказ отменён"), nil
		}
	}

	payOrderRequest := paymentv1.PayOrderRequest{
		OrderUuid:     order.OrderUUID.String(),
		PaymentMethod: toPaymentProto(req.PaymentMethod),
	}

	response, err := h.paymentClient.PayOrder(ctx, &payOrderRequest)
	if err != nil {
		switch status.Code(err) {
		case codes.InvalidArgument:
			return PayOrderBadRequest(), nil
		default:
			return PayOrderInternalServerError(), nil
		}
	}

	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	order, ok = h.store.orders[params.OrderUUID]
	if !ok {
		return PayOrderNotFound(), nil
	}
	if order.Status != OrderStatusPendingPayment {
		return PayOrderInternalServerError(), nil
	}
	valueUUid, err := uuid.Parse(response.TransactionUuid)
	if err != nil {
		return PayOrderInternalServerError(), nil
	}
	paymentMethod := PaymentMethod(req.PaymentMethod)
	order.Status = OrderStatusPaid
	order.TransactionUUID = &valueUUid
	order.PaymentMethod = &paymentMethod
	h.store.orders[params.OrderUUID] = order

	return &orderv1.PayOrderResponse{TransactionUUID: valueUUid}, nil
}

func toPaymentProto(PaymentMethod orderv1.PaymentMethod) paymentv1.PaymentMethod {
	switch PaymentMethod {
	case orderv1.PaymentMethodCARD:
		return paymentv1.PaymentMethod_PAYMENT_METHOD_CARD
	case orderv1.PaymentMethodSBP:
		return paymentv1.PaymentMethod_PAYMENT_METHOD_SBP
	case orderv1.PaymentMethodCREDITCARD:
		return paymentv1.PaymentMethod_PAYMENT_METHOD_CREDIT_CARD
	case orderv1.PaymentMethodINVESTORMONEY:
		return paymentv1.PaymentMethod_PAYMENT_METHOD_INVESTOR_MONEY
	// остальные варианты
	default:
		return paymentv1.PaymentMethod_PAYMENT_METHOD_UNSPECIFIED
	}
}

func CancelOrderNotFound() orderv1.CancelOrderRes {
	return &orderv1.CancelOrderNotFound{
		Code:    404,
		Message: "Заказ не найден",
	}
}

func CancelOrderConflict(message string) orderv1.CancelOrderRes {
	return &orderv1.CancelOrderConflict{
		Code:    409,
		Message: message,
	}
}
func CancelOrderBadRequest() orderv1.CancelOrderRes {
	return &orderv1.CancelOrderBadRequest{
		Code:    400,
		Message: "Некорретно переданные данные",
	}
}
func CancelOrderInternalServerError() orderv1.CancelOrderRes {
	return &orderv1.CancelOrderInternalServerError{
		Code:    500,
		Message: "Некорекстная работа сервера",
	}
}

// CancelOrder реализует операцию cancelOrder
// POST /api/v1/orders/{order_uuid}/cancel
func (h *handler) CancelOrder(ctx context.Context, params orderv1.CancelOrderParams) (orderv1.CancelOrderRes, error) {
	//     // 1. Найти заказ в store
	//     // 2. Проверить статус == PENDING_PAYMENT
	//     // 3. Обновить статус на CANCELLED
	//     // 4. Вернуть success
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	order, ok := h.store.orders[params.OrderUUID]
	if !ok {
		return CancelOrderNotFound(), nil
	}

	if order.Status != OrderStatusPendingPayment {
		switch order.Status {
		case OrderStatusPaid:
			return CancelOrderConflict("Невозможно отменить оплаченный заказ"), nil
		case OrderStatusCancelled:
			return CancelOrderConflict("Заказ уже был отменён"), nil
		default:
			return CancelOrderInternalServerError(), nil
		}
	}
	order.Status = OrderStatusCancelled
	h.store.orders[params.OrderUUID] = order

	return &orderv1.CancelOrderResponse{}, nil
}
