package langgraph

import "errors"

var (
	ErrNoEntryPoint       = errors.New("no entry point defined")
	ErrEntryPointNotFound = errors.New("entry point node not found")
	ErrNodeNotFound       = errors.New("node not found in graph")
	ErrNodeExecution      = errors.New("node execution failed")
	ErrInfiniteLoop       = errors.New("infinite loop detected")
	ErrMaxIterations      = errors.New("max iterations reached")
	ErrNoValidTransition  = errors.New("no valid transition found from node")
)

// State defines the interface that any graph state must implement.
// It allows the graph to control execution flow via ShouldEnd.
type State interface {
	// SetShouldEnd marks that the execution should terminate after the current node.
	SetShouldEnd(bool)
	// GetShouldEnd returns true if execution should terminate.
	GetShouldEnd() bool
}

// DefaultState provides a basic implementation of State
type DefaultState struct {
	shouldEnd bool
}

func (d *DefaultState) SetShouldEnd(v bool) {
	d.shouldEnd = v
}

func (d *DefaultState) GetShouldEnd() bool {
	return d.shouldEnd
}
