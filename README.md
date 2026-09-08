# Reminis

> High-performance, concurrent agent memory layer and DAG orchestrator in pure Go.

## Overview

**Reminis** solves the quadratic context bloat problem in AI agents by treating the LLM as a stateless CPU and orchestrating workflows through an in-memory **Blackboard (Working Memory)** and a **Directed Acyclic Graph (DAG)** of micro-tasks.

Instead of dragging 50,000+ tokens of chat history across multi-step agent runs, Reminis dispatches isolated, ephemeral workers where each request consumes only ~300-800 tokens containing strictly the inputs required for that specific step.

## Key Features

- **Zero Context Degradation:** Each micro-task runs in a clean, isolated context window that is discarded upon completion.
- **Concurrent Execution:** Independent DAG branches execute in parallel via Go goroutines.
- **Thread-safe Blackboard:** Synchronized state passing between task nodes with zero token leakage.
- **Single Static Binary:** Minimal memory footprint (~15-20 MB RAM), zero heavy runtime dependencies.
- **Provider Agnostic:** Connects to any OpenAI-compatible endpoint (local `llama-server`, Ollama, Gemini, Groq, OpenRouter).

## Architecture

See [SPEC.md](SPEC.md) for full architectural design and technical specifications.

## Project Structure

```
reminis/
├── cmd/
│   └── reminis/          # CLI entrypoint
├── internal/
│   ├── blackboard/       # Thread-safe working memory ledger
│   ├── dag/              # DAG scheduler & topological sorting
│   ├── worker/           # Ephemeral LLM worker & prompt synthesizer
│   └── store/            # Durable archival storage
├── pkg/
│   └── client/           # Public client SDK for external agent integration
├── docs/                 # Documentation & diagrams
├── go.mod
├── README.md
└── SPEC.md
```

## License

MIT
