package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/restayway/langgraph-go"
)

// AgentState holds the state of our chatbot
type AgentState struct {
	*langgraph.DefaultState
	Input   string
	History []string
	Output  string
}

func main() {
	// Initialize a new workflow graph
	workflow := langgraph.NewGraph[*AgentState]()

	// Define nodes
	workflow.AddNode("classify", func(ctx context.Context, state *AgentState) (*AgentState, error) {
		fmt.Println("🤖 Classifying intent...")
		if strings.Contains(state.Input, "hello") {
			state.Output = "greeting"
		} else {
			state.Output = "unknown"
		}
		return state, nil
	})

	workflow.AddNode("handle_greeting", func(ctx context.Context, state *AgentState) (*AgentState, error) {
		fmt.Println("👋 Handling greeting...")
		state.History = append(state.History, "User: "+state.Input)
		state.History = append(state.History, "Bot: Hello there!")
		return state, nil
	})

	workflow.AddNode("handle_unknown", func(ctx context.Context, state *AgentState) (*AgentState, error) {
		fmt.Println("❓ Handling unknown input...")
		state.History = append(state.History, "User: "+state.Input)
		state.History = append(state.History, "Bot: I don't understand.")
		return state, nil
	})

	// Define the edges
	workflow.SetEntryPoint("classify")

	// Conditional routing based on classification
	workflow.AddConditionalEdge("classify", func(state *AgentState) string {
		if state.Output == "greeting" {
			return "handle_greeting"
		}
		return "handle_unknown"
	})

	workflow.AddEdge("handle_greeting", langgraph.END)
	workflow.AddEdge("handle_unknown", langgraph.END)

	// Compile the graph
	app, err := workflow.Compile()
	if err != nil {
		panic(err)
	}

	// Run the graph
	initialState := &AgentState{
		DefaultState: &langgraph.DefaultState{},
		Input:        "hello world",
		History:      make([]string, 0),
	}

	fmt.Println("🚀 Starting Agent Workflow...")
	finalState, err := app.Run(context.Background(), initialState)
	if err != nil {
		panic(err)
	}

	fmt.Printf("\n✅ Workflow Completed.\nHistory:\n%s\n", strings.Join(finalState.History, "\n"))
}
