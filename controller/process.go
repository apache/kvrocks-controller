package controller

import (
	"errors"
	"sync"
)

type Process interface {
	LoadTasks() error
	Stop() error
	Close() error
}

type BatchProcessor struct {
	processors     map[string]Process
	processorNames []string
	rw             sync.RWMutex
}

func NewBatchProcessor() *BatchProcessor {
	return &BatchProcessor{
		processors: make(map[string]Process),
	}
}

func (p *BatchProcessor) Lookup(name string) (Process, error) {
	p.rw.Lock()
	defer p.rw.Unlock()
	if _, ok := p.processors[name]; !ok {
		return nil, errors.New(name + "is not register")
	}
	return p.processors[name], nil
}

func (p *BatchProcessor) Register(name string, processor Process) error {
	p.rw.Lock()
	defer p.rw.Unlock()
	p.processors[name] = processor
	p.processorNames = append(p.processorNames, name)
	return nil
}

func (p *BatchProcessor) Start() error {
	p.rw.Lock()
	defer p.rw.Unlock()
	for _, processor := range p.processorNames {
		if err := p.processors[processor].LoadTasks(); err != nil {
			return err
		}
	}
	return nil
}

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

func (p *BatchProcessor) Close() error {
	p.rw.Lock()
	defer p.rw.Unlock()
	for i := len(p.processorNames) - 1; i >= 0; i-- {
		p.processors[p.processorNames[i]].Close()
	}
	return nil
}
