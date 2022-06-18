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

// Processes offer batch Processes to controller
type Processes struct {
	processers    map[string]Process
	processersIdx []string
	rw            sync.RWMutex
}

// NewProcesses return Processes instance
func NewProcesses() *Processes {
	return &Processes{
		processers: make(map[string]Process),
	}
}

// Access return the special Process
func (p *Processes) Access(name string) (Process, error) {
	p.rw.Lock()
	defer p.rw.Unlock()
	if _, ok := p.processers[name]; !ok {
		return nil, errors.New(name + "is not register")
	}
	return p.processers[name], nil
}

// Register when kvrocks_controller start, register Process instance
func (p *Processes) Register(name string, processor Process) error {
	p.rw.Lock()
	defer p.rw.Unlock()
	p.processers[name] = processor
	p.processersIdx = append(p.processersIdx, name)
	return nil
}

// Start batch load Process's data, be called when propose leader
func (p *Processes) Start() error {
	p.rw.Lock()
	defer p.rw.Unlock()
	for _, processer := range p.processersIdx {
		if err := p.processers[processer].LoadData(); err != nil {
			return err
		}
	}
	return nil
}

// Stop batch unload Process's data, be called when up down to folllower
func (p *Processes) Stop() error {
	p.rw.Lock()
	defer p.rw.Unlock()
	for i := len(p.processersIdx) - 1; i >= 0; i-- {
		if err := p.processers[p.processersIdx[i]].Stop(); err != nil {
			return err
		}
	}
	return nil
}

// Close called when controller exist
func (p *Processes) Close() error {
	p.rw.Lock()
	defer p.rw.Unlock()
	for i := len(p.processersIdx) - 1; i >= 0; i-- {
		p.processers[p.processersIdx[i]].Close()
	}
	return nil
}
