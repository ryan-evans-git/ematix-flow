// Current-user session + permission helpers for RBAC UI gating.
// The server enforces RBAC regardless; this only hides/disables
// controls the caller can't use.
import { writable } from "svelte/store";

export const me = writable({
  authenticated: false,
  identity: null,
  role: null,
  permissions: [],
  rbac_enabled: false,
});

export async function loadMe() {
  try {
    const r = await fetch("/api/me", { headers: { Accept: "application/json" } });
    if (r.ok) me.set(await r.json());
  } catch (_) {
    /* leave defaults */
  }
}

// When RBAC is off, everything is permitted (single-tenant / open).
export function can(m, perm) {
  if (!m || !m.rbac_enabled) return true;
  return (m.permissions || []).includes(perm);
}
