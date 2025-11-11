package worker

import (
	"context"
	"github.com/BITS08SATHYA/ares-scheduler/pkg/worker"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	//	 Get WokerId from environment or generate
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		workerID = "worker-1"
	}

	log.Printf("Starting Ares Worker: %s", workerID)

	//	Create Worker
	w, err := worker.NewWorker(workerID, []string{"localhost:2379"})
	if err != nil {
		log.Fatalf("Failed to create worker: %v", err)
	}
	defer w.Close()

	log.Printf("Worker %s connected to etcd", workerID)

	//	Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Printf("Received shutdown signal, stopping worker ....")
		cancel()
	}()

	//	Run Worker
	log.Printf("Worker %s waiting for jobs...", workerID)

	if err := w.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Worker failed: %v", err)
	}

	log.Printf("Worker %s stopped gracefully", workerID)

}
