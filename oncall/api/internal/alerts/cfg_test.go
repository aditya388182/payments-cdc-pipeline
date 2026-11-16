package alerts

import (
	"time"

	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/config"
)

func configForTest() config.Config {
	return config.Config{LagThreshold: 1000, LagFor: 30 * time.Second, FreshnessThreshold: 120 * time.Second,
		FreshnessFor: 30 * time.Second, DLQGrowthThreshold: 5, ConnectorFor: 10 * time.Second}
}
