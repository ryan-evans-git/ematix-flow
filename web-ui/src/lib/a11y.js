// Svelte actions for accessible overlays.
//
// `use:modal={{ onClose }}` on a dialog element:
//   - moves focus into the dialog on open (first focusable, else the
//     dialog itself — give it tabindex="-1"),
//   - traps Tab within the dialog,
//   - closes on Escape via the supplied onClose callback.
//
// Fixes the recurring pattern where a `role="dialog"` opened with no
// keyboard-dismiss, no focus trap, and no initial focus — including the
// destructive "Purge ENTIRE DLQ" confirm.

const FOCUSABLE =
  'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]),' +
  ' textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';

export function modal(node, params = {}) {
  let onClose = params.onClose;

  const focusables = () =>
    Array.from(node.querySelectorAll(FOCUSABLE)).filter(
      (el) => el.offsetParent !== null || el === document.activeElement,
    );

  // Initial focus: first focusable control, else the dialog itself.
  const first = focusables()[0];
  if (first) first.focus();
  else {
    if (!node.hasAttribute("tabindex")) node.setAttribute("tabindex", "-1");
    node.focus();
  }

  function onKeydown(e) {
    if (e.key === "Escape") {
      e.stopPropagation();
      onClose && onClose();
      return;
    }
    if (e.key !== "Tab") return;
    const items = focusables();
    if (items.length === 0) {
      e.preventDefault();
      return;
    }
    const firstEl = items[0];
    const lastEl = items[items.length - 1];
    if (e.shiftKey && document.activeElement === firstEl) {
      e.preventDefault();
      lastEl.focus();
    } else if (!e.shiftKey && document.activeElement === lastEl) {
      e.preventDefault();
      firstEl.focus();
    }
  }

  node.addEventListener("keydown", onKeydown);

  return {
    update(next = {}) {
      onClose = next.onClose;
    },
    destroy() {
      node.removeEventListener("keydown", onKeydown);
    },
  };
}
