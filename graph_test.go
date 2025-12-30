package langgraph_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/restayway/langgraph-go"
)

// TestState implements langgraph.State for testing
type TestState struct {
	*langgraph.DefaultState
	Value    string
	Count    int
	Messages []string
}

func NewTestState(val string) *TestState {
	return &TestState{
		DefaultState: &langgraph.DefaultState{},
		Value:        val,
		Messages:     make([]string, 0),
	}
}

func TestGraph_BasicFlow(t *testing.T) {
	g := langgraph.NewGraph[*TestState]()

	// Define nodes
	g.AddNode("start", func(ctx context.Context, s *TestState) (*TestState, error) {
		s.Messages = append(s.Messages, "start")
		return s, nil
	})

	g.AddNode("process", func(ctx context.Context, s *TestState) (*TestState, error) {
		s.Messages = append(s.Messages, "process")
		s.Value += "_processed"
		return s, nil
	})

	// Define edges
	g.SetEntryPoint("start")
	g.AddEdge("start", "process")
	g.AddEdge("process", langgraph.END)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("Failed to compile: %v", err)
	}

	initial := NewTestState("init")
	final, err := compiled.Run(context.Background(), initial)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if final.Value != "init_processed" {
		t.Errorf("Expected 'init_processed', got '%s'", final.Value)
	}
	if len(final.Messages) != 2 {
		t.Errorf("Expected 2 messages, got %d", len(final.Messages))
	}
}

func TestGraph_ConditionalEdge(t *testing.T) {
	g := langgraph.NewGraph[*TestState]()

	g.AddNode("router", func(ctx context.Context, s *TestState) (*TestState, error) {
		return s, nil
	})

	g.AddNode("left", func(ctx context.Context, s *TestState) (*TestState, error) {
		s.Value = "left"
		return s, nil
	})

	g.AddNode("right", func(ctx context.Context, s *TestState) (*TestState, error) {
		s.Value = "right"
		return s, nil
	})

	g.SetEntryPoint("router")
	g.AddConditionalEdge("router", func(s *TestState) string {
		if s.Value == "go_left" {
			return "left"
		}
		return "right"
	})
	g.AddEdge("left", langgraph.END)
	g.AddEdge("right", langgraph.END)

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	// Test Left
	s1, _ := compiled.Run(context.Background(), NewTestState("go_left"))
	if s1.Value != "left" {
		t.Errorf("Expected left, got %s", s1.Value)
	}

	// Test Right
	s2, _ := compiled.Run(context.Background(), NewTestState("go_right"))
	if s2.Value != "right" {
		t.Errorf("Expected right, got %s", s2.Value)
	}
}

func TestGraph_Validation(t *testing.T) {
	g := langgraph.NewGraph[*TestState]()
	_, err := g.Compile()
	if err != langgraph.ErrNoEntryPoint { // Should fail
		t.Errorf("Expected ErrNoEntryPoint, got %v", err)
	}

	g.SetEntryPoint("non_existent")
	_, err = g.Compile()
	if err != langgraph.ErrEntryPointNotFound {
		t.Errorf("Expected ErrEntryPointNotFound, got %v", err)
	}
}

func TestGraph_Errors(t *testing.T) {
	g := langgraph.NewGraph[*TestState]()

	// 1. Test Node Execution Error
	g.AddNode("start", func(ctx context.Context, s *TestState) (*TestState, error) {
		return s, nil
	})
	g.AddNode("fail", func(ctx context.Context, s *TestState) (*TestState, error) {
		return nil, errors.New("simulated error")
		// I need a proper error here
	})

	g.SetEntryPoint("start")
	g.AddEdge("start", "fail")

	compiled, err := g.Compile()
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	_, err = compiled.Run(context.Background(), NewTestState("init"))
	if err == nil {
		t.Error("Expected execution error, got nil")
	}
	if !strings.Contains(err.Error(), "simulated error") {
		t.Errorf("Expected 'simulated error' in %v", err)
	}
}

// Full coverage suite
func TestGraph_CyclesAndLimits(t *testing.T) {
	g := langgraph.NewGraph[*TestState]()

	g.AddNode("A", func(ctx context.Context, s *TestState) (*TestState, error) {
		s.Count++
		return s, nil
	})

	g.SetEntryPoint("A")
	g.AddEdge("A", "A") // Infinite loop

	compiled, _ := g.Compile()
	_, err := compiled.Run(context.Background(), NewTestState("test"))

	if err == nil {
		t.Error("Expected error for infinite loop, got nil")
	}
	// It relies on visit count > 20
}

func TestGraph_EarlyExit(t *testing.T) {
	g := langgraph.NewGraph[*TestState]()

	g.AddNode("start", func(ctx context.Context, s *TestState) (*TestState, error) {
		s.SetShouldEnd(true)
		return s, nil
	})
	g.AddNode("should_not_run", func(ctx context.Context, s *TestState) (*TestState, error) {
		s.Value = "fail"
		return s, nil
	})

	g.SetEntryPoint("start")
	g.AddEdge("start", "should_not_run")

	compiled, _ := g.Compile()
	final, _ := compiled.Run(context.Background(), NewTestState("init"))

	if final.Value == "fail" {
		t.Error("Graph executed node after SetShouldEnd(true)")
	}
}

func TestGraph_Callback(t *testing.T) {
	g := langgraph.NewGraph[*TestState]()
	g.AddNode("A", func(ctx context.Context, s *TestState) (*TestState, error) { return s, nil })
	g.SetEntryPoint("A")
	g.AddEdge("A", langgraph.END)

	compiled, _ := g.Compile()

	nodesRun := 0
	cb := func(node string, s *TestState) {
		nodesRun++
	}

	compiled.RunWithStream(context.Background(), NewTestState("init"), cb)

	if nodesRun != 1 {
		t.Errorf("Expected 1 callback, got %d", nodesRun)
	}
}

func TestGraph_NoValidTransition(t *testing.T) {
	g := langgraph.NewGraph[*TestState]()
	g.AddNode("A", func(ctx context.Context, s *TestState) (*TestState, error) { return s, nil })
	g.SetEntryPoint("A")
	// No edges from A, so it should exit or error?
	// The code says: if !hasEdges || len(edges) == 0 { return state, nil }
	// So valid end.

	compiled, _ := g.Compile()
	_, err := compiled.Run(context.Background(), NewTestState("init"))
	if err != nil {
		t.Errorf("Expected nil error for leaf node, got %v", err)
	}
}

func TestGraph_ConditionalNoMatch(t *testing.T) {
	g := langgraph.NewGraph[*TestState]()
	g.AddNode("A", func(ctx context.Context, s *TestState) (*TestState, error) { return s, nil })
	g.SetEntryPoint("A")
	g.AddConditionalEdge("A", func(s *TestState) string {
		return "" // Returns empty string, no match
	})

	compiled, _ := g.Compile()
	_, err := compiled.Run(context.Background(), NewTestState("init"))
	if err == nil || !errors.Is(err, langgraph.ErrNoValidTransition) {
		t.Errorf("Expected ErrNoValidTransition, got %v", err)
	}
}

func TestGraph_MissingNodeTarget(t *testing.T) {
	g := langgraph.NewGraph[*TestState]()
	g.AddNode("A", func(ctx context.Context, s *TestState) (*TestState, error) { return s, nil })
	g.SetEntryPoint("A")
	g.AddEdge("A", "B") // B does not exist

	compiled, _ := g.Compile()
	_, err := compiled.Run(context.Background(), NewTestState("init"))
	if err == nil || !errors.Is(err, langgraph.ErrNodeNotFound) {
		t.Errorf("Expected ErrNodeNotFound, got %v", err)
	}
}

func TestGraph_MaxIterations(t *testing.T) {
	g := langgraph.NewGraph[*TestState]()

	// Create a ring of 6 nodes (A-F) to avoid per-node infinite loop detection (20 limit)
	// but hit global max iterations (100 limit)
	nodes := []string{"A", "B", "C", "D", "E", "F"}
	for i, name := range nodes {
		nextNode := nodes[(i+1)%len(nodes)]
		g.AddNode(name, func(ctx context.Context, s *TestState) (*TestState, error) { return s, nil })
		g.AddEdge(name, nextNode)
	}

	g.SetEntryPoint("A")

	compiled, _ := g.Compile()
	_, err := compiled.Run(context.Background(), NewTestState("init"))

	if err == nil || !errors.Is(err, langgraph.ErrMaxIterations) {
		t.Errorf("Expected ErrMaxIterations, got %v", err)
	}
}
