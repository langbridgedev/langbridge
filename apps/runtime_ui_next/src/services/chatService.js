import { resolveAsync } from "./runtimeService.js";
import { langbridgeList } from "./langbridgeApiClient.js";
import { chatProjects, chatThreads } from "../mocks/chat.mock.js";

function normalizeThread(thread) {
  const id = String(thread.id || thread.thread_id || thread.key || thread.title || "thread");
  return {
    id,
    title: thread.title || thread.summary || "Untitled chat",
    path: `/chat/${encodeURIComponent(id)}`,
    createdAt: thread.created_at || thread.createdAt || null,
    updatedAt: thread.updated_at || thread.updatedAt || null,
  };
}

function normalizeAgent(agent) {
  return {
    id: String(agent.id || agent.key || agent.name),
    name: agent.name || "Unnamed agent",
  };
}

export async function listChatThreads() {
  try {
    const items = (await langbridgeList("/api/runtime/v1/threads")).map(normalizeThread);
    if (items.length > 0) {
      return items;
    }
  } catch {}
  return resolveAsync(chatThreads.map((item) => ({ ...item, path: `/chat/${item.id}` })));
}

export async function listAgents() {
  try {
    const items = (await langbridgeList("/api/runtime/v1/agents")).map(normalizeAgent);
    if (items.length > 0) {
      return items;
    }
  } catch {}
  return resolveAsync([]);
}

export function listChatProjects() {
  return resolveAsync(chatProjects);
}
