# Structured LLM Outputs

Langbridge runtime can ask supported LLM providers for schema-validated responses
when agents need structured data for routing, planning, charting, diagnostics,
or markdown artifact assembly.

Configure this per LLM connection with
`llm_connections[].configuration.structured_outputs`.

Supported values:

- `auto` uses provider-native structured output when available, then falls back
  to prompt-based JSON schema parsing. This is the default.
- `native` requires provider-native structured output and fails if the provider
  or endpoint cannot support it.
- `prompt` always uses prompt-based JSON schema parsing.

## OpenAI

OpenAI uses `responses.parse` for native structured output.

```yaml
llm_connections:
  - name: openai_default
    provider: openai
    model: gpt-4o-mini
    api_key_secret:
      provider_type: env
      identifier: OPENAI_API_KEY
    default: true
    configuration:
      structured_outputs: auto
```

For OpenAI-compatible servers that do not support the Responses API, use
`prompt` or leave `auto` enabled so Langbridge can fall back to prompt parsing.

## Azure OpenAI

Azure OpenAI uses `chat.completions.parse` for native structured output.

```yaml
llm_connections:
  - name: azure_default
    provider: azure
    model: gpt-4o
    api_key_secret:
      provider_type: env
      identifier: AZURE_OPENAI_API_KEY
    default: true
    configuration:
      azure_endpoint: https://example.openai.azure.com
      deployment_name: gpt-4o
      api_version: 2024-05-01-preview
      structured_outputs: auto
```

## Ollama

Ollama uses the `/api/chat` `format` JSON schema option for native structured
output.

```yaml
llm_connections:
  - name: local_ollama
    provider: ollama
    model: gpt-oss
    default: true
    configuration:
      base_url: http://localhost:11434
      structured_outputs: auto
      options:
        num_ctx: 8192
```

Ollama connections do not require `api_key` or `api_key_secret`.

## Anthropic

Anthropic uses a forced tool call with the requested JSON schema as the tool
input schema.

```yaml
llm_connections:
  - name: anthropic_default
    provider: anthropic
    model: claude-3-5-sonnet-latest
    api_key_secret:
      provider_type: env
      identifier: ANTHROPIC_API_KEY
    default: true
    configuration:
      structured_outputs: auto
```

Use `native` when you want startup or execution failures to expose unsupported
provider behavior quickly. Use `prompt` when targeting older compatible
endpoints where native structured-output APIs are not available.
