package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/augusto-dmh/drover"
)

func TestRunRetrySuccess(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{
		retryJob: &drover.JobRow{ID: 9, Kind: "email", Queue: "default", State: drover.StateAvailable, Attempt: 0, Args: json.RawMessage(`{}`)},
	}
	var stdout, stderr bytes.Buffer
	code := runRetry(context.Background(), fake, []string{"9"}, false, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit %d stderr=%q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "redrove job 9") {
		t.Fatalf("stdout=%q", stdout.String())
	}
}

func TestRunRetryJSON(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{
		retryJob: &drover.JobRow{ID: 9, State: drover.StateAvailable, Attempt: 0, Args: json.RawMessage(`{}`)},
	}
	var stdout, stderr bytes.Buffer
	code := runRetry(context.Background(), fake, []string{"9"}, true, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit %d", code)
	}
	var got drover.JobRow
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.ID != 9 || got.State != drover.StateAvailable {
		t.Fatalf("got=%+v", got)
	}
}

func TestRunRetryNotFoundExit1(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{retryErr: drover.ErrNotFound}
	var stdout, stderr bytes.Buffer
	code := runRetry(context.Background(), fake, []string{"99"}, false, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("exit %d want 1", code)
	}
	if stderr.Len() == 0 {
		t.Fatal("expected stderr")
	}
}

func TestRunRetryInvalidTransitionExit1(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{retryErr: drover.ErrInvalidTransition}
	var stdout, stderr bytes.Buffer
	code := runRetry(context.Background(), fake, []string{"1"}, false, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("exit %d want 1", code)
	}
}

func TestRunRetryMissingIDExit2(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := runRetry(context.Background(), &fakeInspector{}, nil, false, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("exit %d want 2", code)
	}
}

func TestRunCancelSuccess(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{
		cancelJob: &drover.JobRow{ID: 4, State: drover.StateCancelled, Args: json.RawMessage(`{}`)},
	}
	var stdout, stderr bytes.Buffer
	code := runCancel(context.Background(), fake, []string{"4"}, false, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit %d stderr=%q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "cancelled job 4") {
		t.Fatalf("stdout=%q", stdout.String())
	}
}

func TestRunCancelIneligibleExit1(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{cancelErr: drover.ErrInvalidTransition}
	var stdout, stderr bytes.Buffer
	code := runCancel(context.Background(), fake, []string{"4"}, false, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("exit %d want 1", code)
	}
}

func TestRunEnqueueSuccess(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{
		enqueueJob: &drover.JobRow{ID: 11, Kind: "email", Queue: "mail", Args: json.RawMessage(`{"to":"a"}`), State: drover.StateAvailable},
	}
	var stdout, stderr bytes.Buffer
	code := runEnqueue(context.Background(), fake, []string{"--kind", "email", "--queue", "mail", "--args", `{"to":"a"}`}, false, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit %d stderr=%q", code, stderr.String())
	}
	if !fake.enqueued {
		t.Fatal("Enqueue not called")
	}
	if fake.enqueueKind != "email" || string(fake.enqueueArgs) != `{"to":"a"}` {
		t.Fatalf("kind=%q args=%s", fake.enqueueKind, fake.enqueueArgs)
	}
	if fake.enqueueOpts == nil || fake.enqueueOpts.Queue != "mail" {
		t.Fatalf("opts=%+v", fake.enqueueOpts)
	}
	if !strings.Contains(stdout.String(), "id=11") {
		t.Fatalf("stdout=%q", stdout.String())
	}
}

func TestRunEnqueueDefaultArgs(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{
		enqueueJob: &drover.JobRow{ID: 1, Kind: "ping", Queue: "default", Args: json.RawMessage(`{}`), State: drover.StateAvailable},
	}
	var stdout, stderr bytes.Buffer
	code := runEnqueue(context.Background(), fake, []string{"--kind", "ping"}, true, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit %d stderr=%q", code, stderr.String())
	}
	if string(fake.enqueueArgs) != "{}" {
		t.Fatalf("args=%s want {}", fake.enqueueArgs)
	}
	if fake.enqueueOpts != nil {
		t.Fatalf("unexpected opts %+v", fake.enqueueOpts)
	}
	var got drover.JobRow
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.ID != 1 {
		t.Fatalf("got=%+v", got)
	}
}

func TestRunEnqueueMissingKindNoInsert(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{}
	var stdout, stderr bytes.Buffer
	code := runEnqueue(context.Background(), fake, nil, false, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("exit %d want 2", code)
	}
	if fake.enqueued {
		t.Fatal("must not call Enqueue without kind")
	}
}

func TestRunEnqueueBadJSONNoInsert(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{}
	var stdout, stderr bytes.Buffer
	code := runEnqueue(context.Background(), fake, []string{"--kind", "email", "--args", "{nope"}, false, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("exit %d want 2", code)
	}
	if fake.enqueued {
		t.Fatal("must not call Enqueue with bad JSON")
	}
	if !strings.Contains(stderr.String(), "valid JSON") {
		t.Fatalf("stderr=%q", stderr.String())
	}
}

func TestRunEnqueueInspectorErrorExit1(t *testing.T) {
	t.Parallel()
	fake := &fakeInspector{enqueueErr: errors.New("insert failed")}
	var stdout, stderr bytes.Buffer
	code := runEnqueue(context.Background(), fake, []string{"--kind", "email"}, false, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("exit %d want 1", code)
	}
	if !fake.enqueued {
		t.Fatal("Enqueue should have been attempted")
	}
}

func TestRetryCancelEnqueueViaRun(t *testing.T) {
	t.Parallel()
	getenv := func(string) string { return "postgres://test" }

	t.Run("retry", func(t *testing.T) {
		t.Parallel()
		fake := &fakeInspector{
			retryJob: &drover.JobRow{ID: 1, State: drover.StateAvailable, Args: json.RawMessage(`{}`)},
		}
		open := func(context.Context, string) (inspector, func(), error) { return fake, nil, nil }
		var stdout, stderr bytes.Buffer
		code := run([]string{"retry", "1"}, &stdout, &stderr, getenv, open)
		if code != 0 {
			t.Fatalf("exit %d stderr=%q", code, stderr.String())
		}
	})
	t.Run("cancel", func(t *testing.T) {
		t.Parallel()
		fake := &fakeInspector{
			cancelJob: &drover.JobRow{ID: 2, State: drover.StateCancelled, Args: json.RawMessage(`{}`)},
		}
		open := func(context.Context, string) (inspector, func(), error) { return fake, nil, nil }
		var stdout, stderr bytes.Buffer
		code := run([]string{"cancel", "2"}, &stdout, &stderr, getenv, open)
		if code != 0 {
			t.Fatalf("exit %d stderr=%q", code, stderr.String())
		}
	})
	t.Run("enqueue", func(t *testing.T) {
		t.Parallel()
		fake := &fakeInspector{
			enqueueJob: &drover.JobRow{ID: 3, Kind: "x", Queue: "default", Args: json.RawMessage(`{}`), State: drover.StateAvailable},
		}
		open := func(context.Context, string) (inspector, func(), error) { return fake, nil, nil }
		var stdout, stderr bytes.Buffer
		code := run([]string{"enqueue", "--kind", "x"}, &stdout, &stderr, getenv, open)
		if code != 0 {
			t.Fatalf("exit %d stderr=%q", code, stderr.String())
		}
	})
}
