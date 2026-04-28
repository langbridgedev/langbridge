# Local LLMs

Langbridge runtime supports Ollama as a local LLM provider for runtime agents and
semantic vector embeddings.

Example runtime config:

```yaml
llm_connections:
  - name: local_ollama
    provider: ollama
    model: llama3.1
    default: true
    configuration:
      base_url: http://localhost:11434
      embedding_model: nomic-embed-text
      keep_alive: 5m
      options:
        num_ctx: 8192
```

Ollama connections do not require `api_key` or `api_key_secret`. The Ollama
server must be reachable from the runtime process. For Docker-hosted runtimes,
that usually means using a host-accessible URL rather than `localhost` inside
the container.

## LM Studio And OpenAI-Compatible Servers

LM Studio exposes an OpenAI-compatible local server, so configure it through the
existing `openai` provider and set `configuration.base_url`.

```yaml
llm_connections:
  - name: local_lm_studio
    provider: openai
    model: your-loaded-chat-model
    api_key: lm-studio
    default: true
    configuration:
      base_url: http://localhost:1234/v1
      embedding_model: your-loaded-embedding-model
```

This same pattern works for other local OpenAI-compatible servers when they
support `/v1/chat/completions` and `/v1/embeddings`.
