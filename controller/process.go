package controller

import (
	"errors"
	"sync"
)

// Process be handled by controller
type Process interface {
	// LoadData read data from etcd to memory and start
	LoadData() error

	// Stop inner goroutine and data service
	Stop() error

	// Close be called when exist
	Close() error
}

// BatchProcessor offer batch processor to controller
type BatchProcessor struct {
	processors     map[string]Process
	processorNames []string
	rw             sync.RWMutex
}

// NewBatchProcessor return BatchProcessor instance
func NewBatchProcessor() *BatchProcessor {
	return &BatchProcessor{
		processors: make(map[string]Process),
	}
}

// Lookup return the special Process
func (p *BatchProcessor) Lookup(name string) (Process, error) {
	p.rw.Lock()
	defer p.rw.Unlock()
	if _, ok := p.processors[name]; !ok {
		return nil, errors.New(name + "is not register")
	}
	return p.processors[name], nil
}

// Register when kvrocks_controller start, register Process instance
func (p *BatchProcessor) Register(name string, processor Process) error {
	p.rw.Lock()
	defer p.rw.Unlock()
	p.processors[name] = processor
	p.processorNames = append(p.processorNames, name)
	return nil
}

// Start batch load Process's data, be called when propose leader
func (p *BatchProcessor) Start() error {
	p.rw.Lock()
	defer p.rw.Unlock()
	for _, processor := range p.processorNames {
		if err := p.processors[processor].LoadData(); err != nil {
			return err
		}
	}
	return nil
}

// Stop batch unload Process's data, be called when up down to folllower
func (p *BatchProcessor) Stop() error {
	p.rw.Lock()
	defer p.rw.Unlock()
	for i := len(p.processorNames) - 1; i >= 0; i-- {
		if err := p.processors[p.processorNames[i]].Stop(); err != nil {
			return err
		}
	}
	return nil
}

// Close called when controller exist
func (p *BatchProcessor) Close() error {
	p.rw.Lock()
	defer p.rw.Unlock()
	for i := len(p.processorNames) - 1; i >= 0; i-- {
		p.processors[p.processorNames[i]].Close()
	}
	return nil
}
