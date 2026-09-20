package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	inventoryService "github.com/PinkPig16/msGo/inventory/pkg/service"
	inventoryv1 "github.com/PinkPig16/msGo/shared/pkg/proto/inventory/v1"
)

const grpcAddress = ":50051"

func main() {
	lis, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		slog.Error("не удалось создать listener", "error", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer(grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionIdle:     time.Duration(20) * time.Minute,
		MaxConnectionAge:      time.Duration(15) * time.Minute,
		MaxConnectionAgeGrace: time.Duration(5) * time.Second,
		Time:                  time.Duration(5) * time.Minute,
		Timeout:               time.Duration(20) * time.Second,
	}))

	inventoryv1.RegisterInventoryServiceServer(grpcServer, inventoryService.NewServer())

	// Включаем reflection для postman/grpcurl
	reflection.Register(grpcServer)

	slog.Info("запуск InventoryService", "адрес", grpcAddress)

	cxt, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		err = grpcServer.Serve(lis)
		if err != nil {
			slog.Error("ошибка запуска сервера", "error", err)
			os.Exit(1)
		}
	}()
	<-cxt.Done()
	slog.Info("Сиграл остановки сервера получен, попытка остановить сервер")
	grpcServer.GracefulStop()
	slog.Info("Сервер остановлен")
}
