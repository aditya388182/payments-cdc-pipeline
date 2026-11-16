// Package replayclient is the Go side of the gRPC link to replay-svc: a deadline on every call and retries
// only for UNAVAILABLE (a connection problem, safe to repeat because replays are idempotent).
package replayclient

import (
	"context"
	"time"

	pb "github.com/aditya388182/payments-cdc-pipeline/oncall/api/gen/oncall/v1"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Service is what the API depends on (and what tests fake).
type Service interface {
	Replay(ctx context.Context, dlqID, requestedBy, idempotencyKey string) (*pb.ReplayResponse, error)
	ListDlq(ctx context.Context, cursor string, limit int32, reason string) (*pb.ListDlqResponse, error)
	GetDlq(ctx context.Context, id string) (*pb.GetDlqResponse, error)
	LakeStats(ctx context.Context) (*pb.LakeStatsResponse, error)
	DlqStats(ctx context.Context) (*pb.DlqStatsResponse, error)
}

type Options struct {
	CallTimeout   time.Duration // default 5s
	ReplayTimeout time.Duration // default 90s: replay waits for the stream to apply the record
	MaxAttempts   int           // default 3 (1 try + 2 retries), only on UNAVAILABLE
	Backoff       time.Duration // default 100ms, doubled per retry
	Metrics       *metrics.M
}

type Client struct {
	c    pb.ReplayServiceClient
	opts Options
}

func New(cc grpc.ClientConnInterface, o Options) *Client {
	if o.CallTimeout == 0 {
		o.CallTimeout = 5 * time.Second
	}
	if o.ReplayTimeout == 0 {
		o.ReplayTimeout = 90 * time.Second
	}
	if o.MaxAttempts == 0 {
		o.MaxAttempts = 3
	}
	if o.Backoff == 0 {
		o.Backoff = 100 * time.Millisecond
	}
	return &Client{c: pb.NewReplayServiceClient(cc), opts: o}
}

// Dial connects lazily to addr (plaintext: replay-svc listens on loopback).
func Dial(addr string, o Options) (*Client, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return New(conn, o), conn, nil
}

func call[T any](ctx context.Context, c *Client, method string, timeout time.Duration, fn func(ctx context.Context) (*T, error)) (*T, error) {
	var lastErr error
	backoff := c.opts.Backoff
	for attempt := 1; attempt <= c.opts.MaxAttempts; attempt++ {
		cctx, cancel := context.WithTimeout(ctx, timeout)
		out, err := fn(cctx)
		cancel()
		code := status.Code(err)
		c.opts.Metrics.GRPC(method, code.String())
		if err == nil {
			return out, nil
		}
		lastErr = err
		if code != codes.Unavailable || attempt == c.opts.MaxAttempts {
			return nil, err
		}
		select {
		case <-time.After(backoff):
			backoff *= 2
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	return nil, lastErr
}

func (c *Client) Replay(ctx context.Context, dlqID, by, key string) (*pb.ReplayResponse, error) {
	return call(ctx, c, "Replay", c.opts.ReplayTimeout, func(ctx context.Context) (*pb.ReplayResponse, error) {
		return c.c.Replay(ctx, &pb.ReplayRequest{DlqId: dlqID, RequestedBy: by, IdempotencyKey: key})
	})
}

func (c *Client) ListDlq(ctx context.Context, cursor string, limit int32, reason string) (*pb.ListDlqResponse, error) {
	return call(ctx, c, "ListDlq", c.opts.CallTimeout, func(ctx context.Context) (*pb.ListDlqResponse, error) {
		return c.c.ListDlq(ctx, &pb.ListDlqRequest{Cursor: cursor, Limit: limit, Reason: reason})
	})
}

func (c *Client) GetDlq(ctx context.Context, id string) (*pb.GetDlqResponse, error) {
	return call(ctx, c, "GetDlq", c.opts.CallTimeout, func(ctx context.Context) (*pb.GetDlqResponse, error) {
		return c.c.GetDlq(ctx, &pb.GetDlqRequest{Id: id})
	})
}

func (c *Client) LakeStats(ctx context.Context) (*pb.LakeStatsResponse, error) {
	return call(ctx, c, "LakeStats", c.opts.CallTimeout, func(ctx context.Context) (*pb.LakeStatsResponse, error) {
		return c.c.LakeStats(ctx, &pb.LakeStatsRequest{})
	})
}

func (c *Client) DlqStats(ctx context.Context) (*pb.DlqStatsResponse, error) {
	return call(ctx, c, "DlqStats", c.opts.CallTimeout, func(ctx context.Context) (*pb.DlqStatsResponse, error) {
		return c.c.DlqStats(ctx, &pb.DlqStatsRequest{})
	})
}
