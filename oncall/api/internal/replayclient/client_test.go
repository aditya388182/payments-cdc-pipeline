package replayclient

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/aditya388182/payments-cdc-pipeline/oncall/api/gen/oncall/v1"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

type fake struct {
	pb.UnimplementedReplayServiceServer
	replayCalls  atomic.Int32
	failFirst    int32 // UNAVAILABLE for this many Replay calls
	replayErr    error // returned (after failFirst) when set
	block        bool
	lastDeadline atomic.Int64 // remaining time seen by the server, nanos
}

func (f *fake) Replay(ctx context.Context, r *pb.ReplayRequest) (*pb.ReplayResponse, error) {
	n := f.replayCalls.Add(1)
	if dl, ok := ctx.Deadline(); ok {
		f.lastDeadline.Store(int64(time.Until(dl)))
	}
	if n <= f.failFirst {
		return nil, status.Error(codes.Unavailable, "broker down")
	}
	if f.replayErr != nil {
		return nil, f.replayErr
	}
	if f.block {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &pb.ReplayResponse{Status: pb.ReplayStatus_REPLAY_STATUS_APPLIED, Detail: "ok for " + r.DlqId + "/" + r.IdempotencyKey}, nil
}

func (f *fake) LakeStats(context.Context, *pb.LakeStatsRequest) (*pb.LakeStatsResponse, error) {
	return &pb.LakeStatsResponse{RowCount: 42, MaxLsn: 7}, nil
}

func start(t *testing.T, f *fake, o Options) *Client {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterReplayServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(); srv.Stop(); lis.Close() })
	if o.Backoff == 0 {
		o.Backoff = time.Millisecond
	}
	return New(conn, o)
}

func TestReplayRetriesOnUnavailableThenSucceeds(t *testing.T) {
	f := &fake{failFirst: 2}
	c := start(t, f, Options{})
	r, err := c.Replay(context.Background(), "7", "admin", "key-1")
	if err != nil || r.Status != pb.ReplayStatus_REPLAY_STATUS_APPLIED || r.Detail != "ok for 7/key-1" {
		t.Fatalf("r=%v err=%v", r, err)
	}
	if got := f.replayCalls.Load(); got != 3 {
		t.Fatalf("server saw %d calls, want 3 (1 try + 2 retries)", got)
	}
}

func TestUnavailableForeverGivesUpAfterMaxAttempts(t *testing.T) {
	f := &fake{failFirst: 100}
	c := start(t, f, Options{MaxAttempts: 3})
	_, err := c.Replay(context.Background(), "7", "admin", "k")
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("err = %v", err)
	}
	if got := f.replayCalls.Load(); got != 3 {
		t.Fatalf("calls = %d, want 3", got)
	}
}

func TestOtherErrorsAreNeverRetried(t *testing.T) {
	for _, code := range []codes.Code{codes.InvalidArgument, codes.NotFound, codes.Internal, codes.DeadlineExceeded, codes.FailedPrecondition} {
		f := &fake{replayErr: status.Error(code, "nope")}
		c := start(t, f, Options{})
		_, err := c.Replay(context.Background(), "7", "admin", "k")
		if status.Code(err) != code {
			t.Fatalf("code %v: err = %v", code, err)
		}
		if got := f.replayCalls.Load(); got != 1 {
			t.Fatalf("code %v: %d calls, want exactly 1 (no retry)", code, got)
		}
	}
}

func TestEveryCallCarriesADeadline(t *testing.T) {
	f := &fake{}
	c := start(t, f, Options{ReplayTimeout: 3 * time.Second})
	if _, err := c.Replay(context.Background(), "1", "a", "k"); err != nil {
		t.Fatal(err)
	}
	if d := time.Duration(f.lastDeadline.Load()); d <= 0 || d > 3*time.Second {
		t.Fatalf("server saw deadline %v, want in (0, 3s]", d)
	}
}

func TestSlowServerHitsTheClientDeadlineAndIsNotRetried(t *testing.T) {
	f := &fake{block: true}
	c := start(t, f, Options{ReplayTimeout: 100 * time.Millisecond})
	t0 := time.Now()
	_, err := c.Replay(context.Background(), "1", "a", "k")
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("err = %v", err)
	}
	if time.Since(t0) > 2*time.Second || f.replayCalls.Load() != 1 {
		t.Fatalf("took %v with %d calls", time.Since(t0), f.replayCalls.Load())
	}
}

func TestParentContextCancelStopsRetrying(t *testing.T) {
	f := &fake{failFirst: 100}
	c := start(t, f, Options{Backoff: 200 * time.Millisecond, MaxAttempts: 5})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := c.Replay(ctx, "1", "a", "k")
	if err == nil {
		t.Fatal("expected an error")
	}
	if f.replayCalls.Load() >= 5 {
		t.Fatalf("kept retrying after the context ended (%d calls)", f.replayCalls.Load())
	}
}

func TestLakeStatsRoundTrip(t *testing.T) {
	c := start(t, &fake{}, Options{})
	s, err := c.LakeStats(context.Background())
	if err != nil || s.RowCount != 42 || s.MaxLsn != 7 {
		t.Fatalf("s=%v err=%v", s, err)
	}
}
