export function resolveAsync(value, delayMs = 80) {
  return new Promise((resolve) => {
    const timer = typeof window !== "undefined" ? window.setTimeout : setTimeout;
    timer(() => resolve(value), delayMs);
  });
}
