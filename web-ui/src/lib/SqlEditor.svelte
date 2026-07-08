<script>
  // CodeMirror 6 SQL editor. The CodeMirror modules are dynamically
  // imported in onMount so they land in a separate chunk (Vite splits
  // on dynamic import) — the workflows/runs views never pay for the
  // editor's weight.
  import { onMount, onDestroy, createEventDispatcher } from "svelte";

  export let value = "";
  // Autocomplete schema: { "tableName": ["col1", "col2", ...], ... }
  export let schema = {};
  export let onRun = () => {};

  const dispatch = createEventDispatcher();

  let el;
  let view = null;
  let sqlFn = null;
  let sqlCompartment = null;
  let ready = false;

  onMount(async () => {
    const [cmMod, viewMod, stateMod, sqlMod, darkMod] = await Promise.all([
      import("codemirror"),
      import("@codemirror/view"),
      import("@codemirror/state"),
      import("@codemirror/lang-sql"),
      import("@codemirror/theme-one-dark"),
    ]);
    const { basicSetup } = cmMod;
    const { EditorView, keymap } = viewMod;
    const { EditorState, Compartment } = stateMod;
    const { sql } = sqlMod;
    const { oneDark } = darkMod;

    sqlFn = sql;
    sqlCompartment = new Compartment();

    const runKeymap = keymap.of([
      {
        key: "Mod-Enter",
        preventDefault: true,
        run: () => {
          onRun();
          return true;
        },
      },
    ]);

    const listener = EditorView.updateListener.of((u) => {
      if (u.docChanged) {
        value = u.state.doc.toString();
        dispatch("change", value);
      }
    });

    view = new EditorView({
      parent: el,
      state: EditorState.create({
        doc: value,
        extensions: [
          basicSetup,
          runKeymap,
          sqlCompartment.of(sql({ schema })),
          oneDark,
          listener,
          EditorView.theme({
            "&": { height: "100%", fontSize: "13px" },
            ".cm-scroller": { overflow: "auto", fontFamily: "var(--font-mono, monospace)" },
          }),
        ],
      }),
    });
    ready = true;
  });

  onDestroy(() => {
    if (view) view.destroy();
  });

  // Reconfigure the autocomplete schema when tables/columns load.
  $: if (ready && view && sqlFn && sqlCompartment) {
    view.dispatch({ effects: sqlCompartment.reconfigure(sqlFn({ schema })) });
  }

  // Imperatively load text into the editor (e.g. opening a saved query)
  // without fighting the reactive `value` binding.
  export function setDoc(text) {
    if (view && text !== view.state.doc.toString()) {
      view.dispatch({
        changes: { from: 0, to: view.state.doc.length, insert: text },
      });
      value = text;
    }
  }

  export function getDoc() {
    return view ? view.state.doc.toString() : value;
  }

  // Insert text at the current cursor (schema-browser click-to-insert).
  export function insert(text) {
    if (!view) return;
    const sel = view.state.selection.main;
    view.dispatch({
      changes: { from: sel.from, to: sel.to, insert: text },
      selection: { anchor: sel.from + text.length },
    });
    view.focus();
  }
</script>

<div class="sql-editor" bind:this={el}></div>
{#if !ready}
  <div class="sql-editor-loading">loading editor…</div>
{/if}

<style>
  .sql-editor {
    height: 100%;
    min-height: 0;
    overflow: hidden;
    border: 1px solid var(--border, #2a2a2a);
    border-radius: 6px;
  }
  .sql-editor-loading {
    padding: 8px 12px;
    color: var(--fg-muted, #888);
    font-size: 12px;
  }
</style>
