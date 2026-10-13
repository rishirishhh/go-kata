package main

import (
	"errors"
	"log"
	"net/http"
	"time"
)

type Job struct {
	Handler http.Handler
	Writer  http.ResponseWriter
	Request *http.Request
	Done    chan struct{}
}

type WorkerPool struct {
	jobs chan Job
}

func NewWorkerPool(workerCount, queueSize int) (*WorkerPool, error) {
	if workerCount <= 0 || queueSize <= 0 {
		return nil, errors.New("worker count and queue size must be positive")
	}

	pool := &WorkerPool{
		jobs: make(chan Job, queueSize),
	}

	for i := 0; i < workerCount; i++ {
		go pool.worker()
	}

	return pool, nil
}

func (p *WorkerPool) worker() {
	for job := range p.jobs {
		if job.Request.Context().Err() == nil {
			job.Handler.ServeHTTP(job.Writer, job.Request)
		}
		close(job.Done)
	}
}

func (p *WorkerPool) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		job := Job{
			Handler: next,
			Writer:  w,
			Request: r,
			Done:    make(chan struct{}),
		}

		select {
		case p.jobs <- job:
			<-job.Done
		default:
			http.Error(w, "Server busy", http.StatusServiceUnavailable)
		}
	})
}

func main() {
	const (
		port        = "3000"
		workerCount = 4
		queueSize   = 100
	)

	pool, err := NewWorkerPool(workerCount, queueSize)
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("OK\n"))
	})

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           pool.Handler(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("HTTP server listening on :%s", port)

	if err := server.ListenAndServe(); err != nil &&
		!errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
