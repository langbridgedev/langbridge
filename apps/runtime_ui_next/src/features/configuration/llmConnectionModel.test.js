import test from "node:test";
import assert from "node:assert/strict";

import {
  buildLLMConnectionFormState,
  buildLLMConnectionPayload,
  isRuntimeManagedLLMConnection,
} from "./llmConnectionModel.js";

test("buildLLMConnectionFormState promotes structured output and base URL fields", () => {
  const state = buildLLMConnectionFormState({
    management: "runtime_managed",
    rawPayload: {
      name: "local_openai",
      provider: "openai",
      model: "gpt-4.1-mini",
      description: "OpenAI runtime",
      default: true,
      configuration: {
        base_url: "https://api.openai.com/v1",
        structured_outputs: "native",
        timeout: 30,
      },
    },
  });

  assert.equal(state.name, "local_openai");
  assert.equal(state.provider, "openai");
  assert.equal(state.baseUrl, "https://api.openai.com/v1");
  assert.equal(state.structuredOutputs, "native");
  assert.deepEqual(JSON.parse(state.configurationText), { timeout: 30 });
});

test("buildLLMConnectionPayload creates write-only API key payloads", () => {
  const payload = buildLLMConnectionPayload({
    name: "local_openai",
    provider: "openai",
    model: "gpt-4.1-mini",
    description: "OpenAI runtime",
    apiKey: "sk-test",
    baseUrl: "https://api.openai.com/v1",
    structuredOutputs: "auto",
    isActive: true,
    default: false,
    configurationText: JSON.stringify({ timeout: 30 }),
  });

  assert.deepEqual(payload, {
    name: "local_openai",
    provider: "openai",
    model: "gpt-4.1-mini",
    description: "OpenAI runtime",
    api_key: "sk-test",
    configuration: {
      timeout: 30,
      base_url: "https://api.openai.com/v1",
      structured_outputs: "auto",
    },
    is_active: true,
    default: false,
  });
});

test("buildLLMConnectionPayload omits unchanged API key on update", () => {
  const payload = buildLLMConnectionPayload({
    model: "claude-sonnet-4-6",
    description: "",
    apiKey: "",
    baseUrl: "",
    structuredOutputs: "native",
    isActive: true,
    default: true,
    configurationText: "{}",
  }, { mode: "update" });

  assert.equal(Object.hasOwn(payload, "api_key"), false);
  assert.equal(Object.hasOwn(payload, "name"), false);
  assert.equal(Object.hasOwn(payload, "provider"), false);
  assert.deepEqual(payload.configuration, { structured_outputs: "native" });
});

test("buildLLMConnectionPayload validates non-Ollama API keys on create", () => {
  assert.throws(() => buildLLMConnectionPayload({
    name: "local_openai",
    provider: "openai",
    model: "gpt-4.1-mini",
    apiKey: "",
    configurationText: "{}",
  }), /API key is required/);

  assert.doesNotThrow(() => buildLLMConnectionPayload({
    name: "local_ollama",
    provider: "ollama",
    model: "llama3.2",
    apiKey: "",
    configurationText: "{}",
  }));
});

test("isRuntimeManagedLLMConnection gates editing", () => {
  assert.equal(isRuntimeManagedLLMConnection({ management: "runtime_managed" }), true);
  assert.equal(isRuntimeManagedLLMConnection({ management: "config_managed" }), false);
});
