package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"pg-migrator/internal/api"
	"pg-migrator/internal/config"
	"pg-migrator/internal/crypto"
	"pg-migrator/internal/migrator"
	"pg-migrator/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	box, err := crypto.NewBox(cfg.EncryptionKey)
	if err != nil {
		log.Fatalf("crypto init error: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, cfg.MetaDBURL, box)
	if err != nil {
		log.Fatalf("pg-meta init error: %v", err)
	}
	defer st.Close()

	eng := migrator.NewEngine(st, cfg)
	eng.ResumeActiveMonitors(ctx)

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           api.NewServer(st, eng, cfg),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("[pg-migrator] listening on :%s (state store: PostgreSQL pg-meta)", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("[pg-migrator] shutting down gracefully...")
	eng.Shutdown()
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
}
