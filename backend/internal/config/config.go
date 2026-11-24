package config

import (
	"fmt"
	"os"
)

type Config struct {
	Port                string
	MetaDBURL           string
	EncryptionKey       string
	ToxiproxyURL        string
	PgBouncerControlURL string
	WorkloadStatsURL    string
	LabSourcePassword   string
	LabTargetPassword   string
}

func Load() (*Config, error) {
	metaURL := os.Getenv("META_DB_URL")
	if metaURL == "" {
		return nil, fmt.Errorf("META_DB_URL is required")
	}
	encKey := os.Getenv("APP_ENCRYPTION_KEY")
	if len(encKey) < 16 {
		return nil, fmt.Errorf("APP_ENCRYPTION_KEY must be at least 16 characters")
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	return &Config{
		Port:                port,
		MetaDBURL:           metaURL,
		EncryptionKey:       encKey,
		ToxiproxyURL:        os.Getenv("TOXIPROXY_URL"),
		PgBouncerControlURL: os.Getenv("PGBOUNCER_CONTROL_URL"),
		WorkloadStatsURL:    os.Getenv("WORKLOAD_STATS_URL"),
		LabSourcePassword:   os.Getenv("SOURCE_MIGRATOR_PASSWORD"),
		LabTargetPassword:   os.Getenv("TARGET_MIGRATOR_PASSWORD"),
	}, nil
}
