package orchestrator

import (
	"context"
	"log"
	"time"
)

type ImportStep struct {
	Name string
	Run  func(ctx context.Context) error
}

type ImportChain struct {
	Steps []ImportStep
}

func New() *ImportChain {
	return &ImportChain{}
}

func (c *ImportChain) Add(name string, fn func(ctx context.Context) error) {
	c.Steps = append(c.Steps, ImportStep{
		Name: name,
		Run:  fn,
	})
}

func (c *ImportChain) Run(ctx context.Context) error {
	for i, step := range c.Steps {
		logSection(step.Name)

		start := time.Now()
		if err := step.Run(ctx); err != nil {
			return err
		}

		log.Printf("Step %d completed in %s\n", i+1, time.Since(start))
	}
	return nil
}

func logSection(title string) {
	log.Println("=====================================================")
	log.Println(title)
	log.Println("=====================================================")
}
