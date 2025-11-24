package main

import (
	"context"
	"log"

	"github.com/gin-gonic/gin"
)

func main() {
	cfg := loadConfig()
	db := initDB(cfg.DBPath)
	defer db.Close()

	svc := NewService(cfg, db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go svc.workerLoop(ctx)
	go svc.dispatcherLoop(ctx)

	r := gin.Default()
	r.POST("/consumptions", svc.postConsumptions)
	r.GET("/health", svc.health)
	r.GET("/internal/device-state", svc.getDeviceState)
	r.GET("/internal/queue", svc.getQueue)
	r.GET("/internal/batches", svc.getBatches)
	r.GET("/internal/queue-all", svc.getQueueAll)

	log.Printf("Consumption Service on :8080 (DB=%s)", cfg.DBPath)
	if err := r.Run(":8080"); err != nil {
		log.Fatal(err)
	}
}
