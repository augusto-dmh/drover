//go:build integration

// Package testdb owns the PostgreSQL testcontainer shared by a test
// package and hands each test an isolated, freshly created database,
// so tests never observe each other's rows or schema state.
package testdb

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	adminURL string
	dbSeq    atomic.Int64
)

// RunMain starts one PostgreSQL container for the package's tests,
// runs them, and tears the container down. Call from TestMain:
//
//	func TestMain(m *testing.M) { os.Exit(testdb.RunMain(m)) }
func RunMain(m *testing.M) int {
	ctx := context.Background()

	container, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("postgres"),
		postgres.WithUsername("drover"),
		postgres.WithPassword("drover"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "testdb: start postgres container: %v\n", err)
		return 1
	}
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "testdb: terminate container: %v\n", err)
		}
	}()

	adminURL, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "testdb: connection string: %v\n", err)
		return 1
	}
	return m.Run()
}

// NewDB creates a fresh database in the shared container and returns a
// pool connected to it. Pool and database are cleaned up with the test.
func NewDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	admin, err := pgxpool.New(ctx, adminURL)
	if err != nil {
		t.Fatalf("testdb: connect admin pool: %v", err)
	}

	name := fmt.Sprintf("drover_test_%d", dbSeq.Add(1))
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		admin.Close()
		t.Fatalf("testdb: create database %s: %v", name, err)
	}

	dbURL, err := url.Parse(adminURL)
	if err != nil {
		admin.Close()
		t.Fatalf("testdb: parse admin url: %v", err)
	}
	dbURL.Path = "/" + name

	pool, err := pgxpool.New(ctx, dbURL.String())
	if err != nil {
		admin.Close()
		t.Fatalf("testdb: connect to %s: %v", name, err)
	}

	t.Cleanup(func() {
		pool.Close()
		if _, err := admin.Exec(context.Background(), "DROP DATABASE "+name); err != nil {
			t.Errorf("testdb: drop database %s: %v", name, err)
		}
		admin.Close()
	})
	return pool
}
