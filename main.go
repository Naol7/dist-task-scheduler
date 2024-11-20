package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Task represents a unit of work with retry count.
type Task struct {
	id         int
	data       string
	retryCount int
}

// MaxRetries defines how many times a task can be retried.
const MaxRetries = 3

// Worker function that processes tasks. If a worker fails, the task will be sent to failChan.
func worker(id int, taskChan <-chan Task, wg *sync.WaitGroup, failChan chan<- Task) {
	defer wg.Done()

	for task := range taskChan {
		fmt.Printf("Worker %d started task %d: %s\n", id, task.id, task.data)

		// Simulate random failure (30% chance of failure)
		if rand.Float32() < 0.3 {
			fmt.Printf("Worker %d failed on task %d\n", id, task.id)
			failChan <- task // Send the failed task for reassignment
			return
		}

		// Simulate task processing time
		time.Sleep(time.Duration(rand.Intn(3)+1) * time.Second)
		fmt.Printf("Worker %d completed task %d\n", id, task.id)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Define a set of tasks to be executed
	tasks := []Task{
		{id: 1, data: "Task 1"},
		{id: 2, data: "Task 2"},
		{id: 3, data: "Task 3"},
		{id: 4, data: "Task 4"},
		{id: 5, data: "Task 5"},
	}

	// Channels for task distribution and failure handling
	taskChan := make(chan Task, len(tasks))
	failChan := make(chan Task, len(tasks))

	// WaitGroup to ensure all workers finish their tasks
	var wg sync.WaitGroup

	// Number of workers (simulating processors)
	numWorkers := 3

	// Counter for round-robin task assignment
	workerCounter := 0

	// Map to track retry counts for each task
	taskRetryMap := make(map[int]int)

	// Start worker goroutines
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(i, taskChan, &wg, failChan)
	}

	// Distribute tasks to workers in a round-robin manner
	for _, task := range tasks {
		taskRetryMap[task.id] = 0 // Initialize retry count
		taskChan <- task
	}
	close(taskChan)

	// Handle failed tasks by redistributing them with retry limits
	go func() {
		for failedTask := range failChan {
			taskRetryMap[failedTask.id]++

			// Check if the task has exceeded the maximum retry limit
			if taskRetryMap[failedTask.id] > MaxRetries {
				fmt.Printf("Task %d has exceeded the retry limit of %d and will not be retried further.\n", failedTask.id, MaxRetries)
				continue
			}

			fmt.Printf("Reassigning failed task %d (Retry %d)\n", failedTask.id, taskRetryMap[failedTask.id])

			// Assign the task to the next worker in a round-robin manner
			workerCounter = (workerCounter % numWorkers) + 1
			wg.Add(1)
			go worker(workerCounter, taskChan, &wg, failChan)
		}
	}()

	// Wait for all workers to finish
	wg.Wait()
	close(failChan)

	fmt.Println("All tasks completed.")
}
