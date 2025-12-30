package langgraph

import (
	"context"
	"fmt"
)

// END is the constant used to represent the end of the workflow.
const END = "__end__"

// NodeFunc is the signature for a node's execution logic.
type NodeFunc[S State] func(ctx context.Context, state S) (S, error)

// ConditionalFunc determines which node to transition to next based on the state.
type ConditionalFunc[S State] func(state S) string

// Edge represents a transition between nodes.
type Edge[S State] struct {
	From        string
	To          string
	Conditional ConditionalFunc[S]
}

// Graph is the core structure for building workflows.
type Graph[S State] struct {
	nodes      map[string]NodeFunc[S]
	edges      []Edge[S]
	entryPoint string
	// nodeOrder is maintained for deterministic behaviors or debugging
	nodeOrder []string
}

// NewGraph creates a new instance of a Graph.
func NewGraph[S State]() *Graph[S] {
	return &Graph[S]{
		nodes:     make(map[string]NodeFunc[S]),
		edges:     make([]Edge[S], 0),
		nodeOrder: make([]string, 0),
	}
}

// AddNode registers a function as a node in the graph.
func (g *Graph[S]) AddNode(name string, fn NodeFunc[S]) *Graph[S] {
	g.nodes[name] = fn
	g.nodeOrder = append(g.nodeOrder, name)
	return g
}

// AddEdge creates a direct transition from one node to another.
func (g *Graph[S]) AddEdge(from, to string) *Graph[S] {
	g.edges = append(g.edges, Edge[S]{From: from, To: to})
	return g
}

// AddConditionalEdge creates a dynamic transition where the next node is determined at runtime.
func (g *Graph[S]) AddConditionalEdge(from string, condition ConditionalFunc[S]) *Graph[S] {
	g.edges = append(g.edges, Edge[S]{From: from, Conditional: condition})
	return g
}

// SetEntryPoint defines the node where execution begins.
func (g *Graph[S]) SetEntryPoint(name string) *Graph[S] {
	g.entryPoint = name
	return g
}

// CompiledGraph represents a graph that has been validated and optimized for execution.
type CompiledGraph[S State] struct {
	nodes      map[string]NodeFunc[S]
	adjacency  map[string][]Edge[S]
	entryPoint string
}

// Compile validates the graph structure and prepares it for execution.
func (g *Graph[S]) Compile() (*CompiledGraph[S], error) {
	if g.entryPoint == "" {
		return nil, ErrNoEntryPoint
	}

	if _, ok := g.nodes[g.entryPoint]; !ok {
		return nil, ErrEntryPointNotFound
	}

	// Build adjacency map for O(1) lookups during traversal
	adjacency := make(map[string][]Edge[S])
	for _, edge := range g.edges {
		adjacency[edge.From] = append(adjacency[edge.From], edge)
	}

	return &CompiledGraph[S]{
		nodes:      g.nodes,
		adjacency:  adjacency,
		entryPoint: g.entryPoint,
	}, nil
}

// Run executes the graph starting from the entry point with the given state.
func (cg *CompiledGraph[S]) Run(ctx context.Context, initialState S) (S, error) {
	// Simple wrapper for RunWithStream with no callback
	return cg.RunWithStream(ctx, initialState, nil)
}

// NodeCallback is a function called after each node execution, useful for tracing/logging.
type NodeCallback[S State] func(nodeName string, state S)

// RunWithStream executes the graph and calls the callback after every node execution.
func (cg *CompiledGraph[S]) RunWithStream(ctx context.Context, initialState S, callback NodeCallback[S]) (S, error) {
	state := initialState
	currentNode := cg.entryPoint
	visited := make(map[string]int)

	// Safety break to prevent infinite loops if logic is flawed
	// This could be configurable in a real SDK options struct
	const maxIterations = 100

	for i := 0; i < maxIterations; i++ {
		// 1. Check infinite loop on single node (re-visiting same node too many times in a loop)
		visited[currentNode]++
		if visited[currentNode] > 20 {
			return state, fmt.Errorf("%w: node %s visited too many times", ErrInfiniteLoop, currentNode)
		}

		// 2. Get Node Function
		nodeFn, ok := cg.nodes[currentNode]
		if !ok {
			return state, fmt.Errorf("%w: %s", ErrNodeNotFound, currentNode)
		}

		// 3. Execute Node
		var err error
		state, err = nodeFn(ctx, state)
		if err != nil {
			return state, fmt.Errorf("%w: %v", ErrNodeExecution, err)
		}

		// 4. Invoke Callback
		if callback != nil {
			callback(currentNode, state)
		}

		// 5. Check if state requests termination
		if state.GetShouldEnd() {
			return state, nil
		}

		// 6. Determine Next Node
		edges, hasEdges := cg.adjacency[currentNode]
		if !hasEdges || len(edges) == 0 {
			// No edges implying end of workflow
			return state, nil
		}

		nextNode := ""
		for _, edge := range edges {
			if edge.Conditional != nil {
				// Eval conditional edge
				target := edge.Conditional(state)
				if target != "" {
					nextNode = target
					break
				}
			} else {
				// Standard edge
				nextNode = edge.To
				break
			}
		}

		if nextNode == "" {
			return state, fmt.Errorf("%w: from %s", ErrNoValidTransition, currentNode)
		}

		if nextNode == END {
			return state, nil
		}

		currentNode = nextNode
	}

	return state, ErrMaxIterations
}
