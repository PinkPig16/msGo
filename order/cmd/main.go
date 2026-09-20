package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	orderHandler "github.com/PinkPig16/msGo/order/pkg/handler"
	inventoryv1 "github.com/PinkPig16/msGo/shared/pkg/proto/inventory/v1"
	paymentv1 "github.com/PinkPig16/msGo/shared/pkg/proto/payment/v1"
)

const (
	inventoryServiceAddress = "localhost:50051"
	paymentServiceAddress   = "localhost:50052"
)

func main() {
	// Подумайте, какие параметры стоит задать для gRPC клиента
	// См. examples/week_1/GRPC_CONNECTIONS.md

	// Создать gRPC соединение с InventoryService
	inventoryConn, err := grpc.NewClient(inventoryServiceAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Duration(30) * time.Second,
			PermitWithoutStream: true,
			Timeout:             time.Duration(25) * time.Second,
		}))
	if err != nil {
		slog.Error("не удалось подключиться к InventoryService", "error", err)
		os.Exit(1)
	}
	defer inventoryConn.Close()

	paymentConn, err := grpc.NewClient(paymentServiceAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Duration(30) * time.Second,
			PermitWithoutStream: true,
			Timeout:             time.Duration(25) * time.Second,
		}))
	if err != nil {
		slog.Error("не удалось подключиться к PaymentService", "error", err)
		os.Exit(1)
	}
	defer paymentConn.Close()

	// Создаём хранилище и обработчик
	store := orderHandler.NewOrderStore()
	h := orderHandler.NewHandler(
		inventoryv1.NewInventoryServiceClient(inventoryConn),
		paymentv1.NewPaymentServiceClient(paymentConn),
		store,
	)

	// Команда: task ogen:gen
	// Создать OpenAPI сервер
	httpServe, err := orderHandler.SetupServer(h)
	if err != nil {
		slog.Error("ошибка создания сервера OpenAPI", "error", err)
		os.Exit(1)
	}

	// Создайте &http.Server{...} с явными таймаутами вместо http.ListenAndServe(...)
	// Минимальный набор: ReadHeaderTimeout (защита от Slowloris), ReadTimeout, WriteTimeout, IdleTimeout
	// Без ReadHeaderTimeout сервер уязвим к атаке Slowloris (медленная отправка заголовков)
	// См. examples/week_1/HTTP_SERVER.md
	server := &http.Server{
		Addr:              ":8080",
		Handler:           httpServe,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1 MB
	}

	// При получении сигнала SIGINT/SIGTERM сервер должен:
	// 1. Перестать принимать новые соединения
	// 2. Дождаться завершения текущих запросов (с таймаутом)
	// 3. Закрыть gRPC соединения
	// 4. Корректно завершить работу
	// Подсказка: используйте signal.NotifyContext и httpServer.Shutdown(ctx)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	slog.Info("запуск OrderService", "port", 8080)

	go func() {
		err = server.ListenAndServe()
		if err != nil {
			slog.Error("ошибка запуска сервера", "error", err)
			os.Exit(1)
		}
	}()
	<-ctx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if shutdownErr := server.Shutdown(shutdownCtx); shutdownErr != nil {
		slog.Error("ошибка остановки HTTP сервера", "error", shutdownErr)
	}

	slog.Info("✅ HTTP сервер остановлен")
}
