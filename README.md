# LangGraph-Go

A lightweight, generic graph-based workflow engine for Go, inspired by LangGraph. This SDK allows you to build stateful, multi-step workflows with cycles, conditional branching, and human-in-the-loop capabilities (via execution control).

## Features

- 🕸 **Graph-based Workflow**: Define nodes and edges to create complex execution flows.
- 🔀 **Conditional Branching**: Dynamically route execution based on state.
- 🔁 **Cycles Support**: Built-in loop detection and iteration limits.
- 💾 **State Management**: Strongly typed generic state passing between nodes.
- 🛑 **Early Termination**: Control execution flow explicitly from nodes.
- 📡 **Streaming Callbacks**: Hook into execution for logging or real-time updates.

## Installation

```bash
go get github.com/restayway/langgraph-go
```

## Quick Start

Here's a simple example of how to define and run a graph:

```go
package main

import (
    "context"
    "fmt"
    "github.com/restayway/langgraph-go"
)

// 1. Define your Custom State
type MyState struct {
    *langgraph.DefaultState
    Data string
}

func main() {
    // 2. Create the Graph
    g := langgraph.NewGraph[*MyState]()

    // 3. Add Nodes
    g.AddNode("start", func(ctx context.Context, s *MyState) (*MyState, error) {
        fmt.Println("Starting processing:", s.Data)
        s.Data += "_processed"
        return s, nil
    })

    // 4. Define Edges
    g.SetEntryPoint("start")
    g.AddEdge("start", langgraph.END)

    // 5. Compile
    app, _ := g.Compile()

    // 6. Run
    final, _ := app.Run(context.Background(), &MyState{
        DefaultState: &langgraph.DefaultState{},
        Data: "input",
    })

    fmt.Println(final.Data) // Output: input_processed
}
```

Check the `examples/` directory for more complex use cases like conditional routing (chatbots).

## Documentation

### Nodes
A node is simply a function that takes the current state and returns a verified state.
```go
type NodeFunc[S State] func(ctx context.Context, state S) (S, error)
```

### Edges
Edges define the transition between nodes.
- **AddEdge(from, to)**: Direct transition.
- **AddConditionalEdge(from, conditionFunc)**: Dynamic transition.

### State Interface
Your state struct must implement the `State` interface to allow the engine to control flow (e.g., stop execution).
```go
type State interface {
    SetShouldEnd(bool)
    GetShouldEnd() bool
}
```
You can embed `langgraph.DefaultState` to handle this automatically.
