import { langbridgeList } from "./langbridgeApiClient.js";

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
  return (await langbridgeList("/api/runtime/v1/threads")).map(normalizeThread);
}

export async function listAgents() {
  return (await langbridgeList("/api/runtime/v1/agents")).map(normalizeAgent);
}

export function listChatProjects() {
  return Promise.resolve([]);
}
