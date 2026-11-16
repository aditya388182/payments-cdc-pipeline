package collect

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/config"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/metrics"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/replayclient"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/snapshot"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Collectors run one goroutine per source. Every outbound call has a timeout; a failing source only marks
// itself unhealthy in the State, it never blocks the snapshot loop.
type Collectors struct {
	Cfg    config.Config
	State  *State
	Replay replayclient.Service
	Source *pgxpool.Pool
	M      *metrics.M
	HTTP   *http.Client
}

func (c *Collectors) Run(ctx context.Context) {
	if c.HTTP == nil {
		c.HTTP = &http.Client{Timeout: 2 * time.Second}
	}
	var wg sync.WaitGroup
	every := func(name string, d time.Duration, fn func(ctx context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t := time.NewTicker(d)
			defer t.Stop()
			for {
				cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
				if err := fn(cctx); err != nil && ctx.Err() == nil {
					c.State.Fail(name, time.Now(), err)
					c.M.CollectorError(name)
					slog.Warn("collector failed", "source", name, "err", err.Error())
				}
				cancel()
				select {
				case <-ctx.Done():
					return
				case <-t.C:
				}
			}
		}()
	}
	lag := c.newLagCollector()
	every("kafka", 2*time.Second, lag.collect)
	every("connect", 2*time.Second, c.collectConnector)
	every("source_db", 2*time.Second, c.collectSourceCount)
	every("lake", 2*time.Second, c.collectLake)
	every("dlq", 5*time.Second, c.collectDLQ)
	wg.Add(1)
	go func() { defer wg.Done(); c.consumeProgress(ctx) }()
	wg.Wait()
	lag.close()
}

// ---- Kafka lag -----------------------------------------------------------------------------------
// Spark Structured Streaming keeps its offsets in its own checkpoint and never commits a consumer group, so
// the admin API only knows the END offsets. "Processed" offsets are the ones the job already publishes to the
// Pushgateway (payments_cdc_processed_offset{partition}), the same series the repo's Grafana dashboards use.

type lagCollector struct {
	c   *Collectors
	cl  *kgo.Client
	adm *kadm.Client
}

func (c *Collectors) newLagCollector() *lagCollector {
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.Cfg.KafkaBrokers...), kgo.RequestRetries(1), kgo.DialTimeout(2*time.Second))
	if err != nil {
		slog.Error("kafka admin client", "err", err.Error())
		return &lagCollector{c: c}
	}
	return &lagCollector{c: c, cl: cl, adm: kadm.NewClient(cl)}
}

func (l *lagCollector) close() {
	if l.cl != nil {
		l.cl.Close()
	}
}

func (l *lagCollector) collect(ctx context.Context) error {
	if l.adm == nil {
		return errors.New("kafka client not available")
	}
	ends, err := l.adm.ListEndOffsets(ctx, l.c.Cfg.CDCTopic)
	if err != nil {
		return fmt.Errorf("list end offsets: %w", err)
	}
	if err := ends.Error(); err != nil {
		return fmt.Errorf("end offsets: %w", err)
	}
	pushgw, err := l.c.scrapeProcessedOffsets(ctx)
	if err != nil {
		slog.Debug("pushgateway fallback unavailable", "err", err.Error())
		pushgw = nil // progress events are the primary source; the Pushgateway is only a fallback
	}
	endMap := map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) { endMap[o.Partition] = o.Offset })
	parts := ComputeLag(endMap, l.c.State.Processed(), pushgw)
	var endTotal int64
	for _, e := range endMap {
		endTotal += e
	}
	sortParts(parts)
	l.c.State.SetLag(time.Now(), parts, endTotal)
	return nil
}

// ComputeLag is end offset minus processed offset per partition. The processed offset comes from the Spark
// progress events (exact, taken from the raw Kafka batch at commit time); only for a partition the events have
// not mentioned yet does it fall back to the Pushgateway series, which is always one batch stale after the last
// batch (so it would show a permanent lag of one batch while idle). A partition known to neither is all lag.
func ComputeLag(ends, fromEvents, fromPushgateway map[int32]int64) []snapshot.PartitionLag {
	var parts []snapshot.PartitionLag
	for p, end := range ends {
		proc, ok := fromEvents[p]
		if !ok {
			proc = fromPushgateway[p]
		}
		lag := end - proc
		if lag < 0 {
			lag = 0
		}
		parts = append(parts, snapshot.PartitionLag{Partition: p, End: end, Processed: proc, Lag: lag})
	}
	sortParts(parts)
	return parts
}

func sortParts(p []snapshot.PartitionLag) {
	for i := 1; i < len(p); i++ {
		for j := i; j > 0 && p[j].Partition < p[j-1].Partition; j-- {
			p[j], p[j-1] = p[j-1], p[j]
		}
	}
}

var processedRe = regexp.MustCompile(`^payments_cdc_processed_offset\{[^}]*partition="(\d+)"[^}]*\}\s+([0-9.eE+-]+)`)

// ParseProcessedOffsets extracts payments_cdc_processed_offset{partition="N"} values from Prometheus text.
func ParseProcessedOffsets(r io.Reader) map[int32]int64 {
	out := map[int32]int64{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		m := processedRe.FindStringSubmatch(sc.Text())
		if m == nil {
			continue
		}
		p, _ := strconv.Atoi(m[1])
		v, err := strconv.ParseFloat(m[2], 64)
		if err != nil {
			continue
		}
		out[int32(p)] = int64(v)
	}
	return out
}

func (c *Collectors) scrapeProcessedOffsets(ctx context.Context) (map[int32]int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.Cfg.PushgatewayURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("pushgateway: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("pushgateway: HTTP %d", resp.StatusCode)
	}
	return ParseProcessedOffsets(io.LimitReader(resp.Body, 4<<20)), nil
}

// ---- Kafka Connect -----------------------------------------------------------------------------

type connectStatus struct {
	Connector struct {
		State string `json:"state"`
	} `json:"connector"`
	Tasks []struct {
		State string `json:"state"`
	} `json:"tasks"`
}

// ConnectorFromStatus collapses Connect's status document into one state: the connector's own state if it is
// not RUNNING, else the first task that is not RUNNING, else RUNNING.
func ConnectorFromStatus(raw []byte) (snapshot.Connector, error) {
	var s connectStatus
	if err := json.Unmarshal(raw, &s); err != nil {
		return snapshot.Connector{}, err
	}
	out := snapshot.Connector{State: s.Connector.State}
	for _, t := range s.Tasks {
		out.Tasks = append(out.Tasks, t.State)
	}
	if out.State == "RUNNING" {
		for _, t := range out.Tasks {
			if t != "RUNNING" {
				out.State = t
				break
			}
		}
	}
	return out, nil
}

func (c *Collectors) collectConnector(ctx context.Context) error {
	url := fmt.Sprintf("%s/connectors/%s/status", c.Cfg.ConnectURL, c.Cfg.ConnectorName)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		c.State.SetConnector(time.Now(), snapshot.Connector{State: "NOT_REGISTERED"})
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("connect: HTTP %d", resp.StatusCode)
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	conn, err := ConnectorFromStatus(b)
	if err != nil {
		return err
	}
	c.State.SetConnector(time.Now(), conn)
	return nil
}

// ---- source / lake / DLQ ---------------------------------------------------------------------------

func (c *Collectors) collectSourceCount(ctx context.Context) error {
	var n int64
	if err := c.Source.QueryRow(ctx, `select count(*) from transactions`).Scan(&n); err != nil {
		return fmt.Errorf("source count: %w", err)
	}
	c.State.SetSourceCount(time.Now(), n)
	return nil
}

func (c *Collectors) collectLake(ctx context.Context) error {
	s, err := c.Replay.LakeStats(ctx)
	if err != nil {
		return fmt.Errorf("lake stats: %w", err)
	}
	c.State.SetLake(time.Now(), s.RowCount)
	return nil
}

func (c *Collectors) collectDLQ(ctx context.Context) error {
	s, err := c.Replay.DlqStats(ctx)
	if err != nil {
		return fmt.Errorf("dlq stats: %w", err)
	}
	c.State.SetDLQ(time.Now(), s.TopicMessages, s.OpenTotal, s.OpenMissingKeys)
	return nil
}

// ---- Spark progress events ---------------------------------------------------------------------

func (c *Collectors) consumeProgress(ctx context.Context) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.Cfg.KafkaBrokers...), kgo.ConsumeTopics(c.Cfg.ProgressTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd().Relative(-30)), kgo.DialTimeout(2*time.Second))
	if err != nil {
		slog.Error("progress consumer", "err", err.Error())
		return
	}
	defer cl.Close()
	for ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}
		fetches.EachError(func(topic string, p int32, err error) {
			if !errors.Is(err, context.Canceled) {
				slog.Warn("progress fetch", "topic", topic, "partition", p, "err", err.Error())
				c.M.CollectorError("progress")
			}
		})
		fetches.EachRecord(func(r *kgo.Record) {
			var ev ProgressEvent
			if err := json.Unmarshal(r.Value, &ev); err != nil {
				slog.Warn("progress: bad event", "err", err.Error())
				return
			}
			at := time.Now()
			if t, err := time.Parse(time.RFC3339Nano, ev.CommittedAt); err == nil {
				at = t // same host/clock: use the commit time so replayed history is not mistaken for fresh activity
			}
			c.State.ObserveBatch(at, ev)
		})
	}
}
