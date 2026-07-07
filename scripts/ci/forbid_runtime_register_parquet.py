#!/usr/bin/env python3
"""Guard: shipped runtime code must register TPC-H/parquet tables via the
hand-rolled ematix-parquet reader (`EmatixFastParquetTableProvider`), never
DataFusion's arrow-rs `register_parquet` / `ListingTable`. ematix-parquet is a
core differentiator (late-materialization + fused predicate kernels); a silent
drift to arrow-rs on the shipped path is exactly the bug that made the AWS
campaign under-measure the engine (2026-07). This forbids `register_parquet(` in
non-test runtime code under `crates/*/src/`.

ALLOWED (not scanned / stripped before matching):
  - `#[cfg(test)]` modules and items (unit tests may use register_parquet freely)
  - line, doc, and block comments
  - string/char literals
  - anything outside `crates/*/src/` (examples/, benches/, tests/ are not shipped)

Usage:
  forbid_runtime_register_parquet.py [--root DIR]   # scan; exit 1 on violation
  forbid_runtime_register_parquet.py --self-test     # run built-in fixtures
"""
from __future__ import annotations
import sys
import re
from pathlib import Path

NEEDLE = "register_parquet("


def strip_comments_and_literals(src: str) -> str:
    """Return src with comment bodies and string/char-literal bodies replaced by
    spaces (newlines preserved so line numbers are stable). Braces that live in
    real code are kept intact so a later brace-matcher is reliable."""
    out = []
    i, n = 0, len(src)
    state = "code"  # code | line_comment | block_comment | string | char | raw_string
    raw_hashes = 0
    while i < n:
        c = src[i]
        nxt = src[i + 1] if i + 1 < n else ""
        if state == "code":
            # raw string r"..." or r#"..."#
            if c == "r" and (nxt == '"' or nxt == "#"):
                j = i + 1
                hashes = 0
                while j < n and src[j] == "#":
                    hashes += 1
                    j += 1
                if j < n and src[j] == '"':
                    raw_hashes = hashes
                    state = "raw_string"
                    out.append(" ")
                    i = j + 1
                    continue
            if c == "/" and nxt == "/":
                state = "line_comment"; out.append("  "); i += 2; continue
            if c == "/" and nxt == "*":
                state = "block_comment"; out.append("  "); i += 2; continue
            if c == '"':
                state = "string"; out.append(" "); i += 1; continue
            if c == "'":
                # char literal vs lifetime: a lifetime is 'ident with no closing
                # quote soon. Treat 'x' / '\n' as char; else pass through.
                m = re.match(r"'(\\.|[^'\\])'", src[i:])
                if m:
                    out.append(" " * len(m.group(0))); i += len(m.group(0)); continue
                out.append(c); i += 1; continue
            out.append(c); i += 1; continue
        if state == "line_comment":
            if c == "\n":
                state = "code"; out.append("\n")
            else:
                out.append(" ")
            i += 1; continue
        if state == "block_comment":
            if c == "*" and nxt == "/":
                state = "code"; out.append("  "); i += 2; continue
            out.append("\n" if c == "\n" else " "); i += 1; continue
        if state == "string":
            if c == "\\":
                out.append("  "); i += 2; continue
            if c == '"':
                state = "code"; out.append(" "); i += 1; continue
            out.append("\n" if c == "\n" else " "); i += 1; continue
        if state == "raw_string":
            if c == '"':
                j = i + 1
                hashes = 0
                while j < n and src[j] == "#" and hashes < raw_hashes:
                    hashes += 1; j += 1
                if hashes == raw_hashes:
                    state = "code"; out.append(" " * (1 + hashes)); i = j; continue
            out.append("\n" if c == "\n" else " "); i += 1; continue
    return "".join(out)


def remove_cfg_test_blocks(code: str) -> str:
    """Blank out `#[cfg(test)]`-attributed items by brace-matching the block that
    follows the attribute (its `mod tests { ... }` or `fn ... { ... }`). Newlines
    are preserved so violation line numbers stay accurate."""
    result = list(code)
    for m in re.finditer(r"#\[cfg\(test\)\]", code):
        # find the next opening brace after the attribute
        j = m.end()
        while j < len(code) and code[j] != "{":
            # stop if we hit a `;` (attribute on a non-block item) — rare; skip
            if code[j] == ";":
                break
            j += 1
        if j >= len(code) or code[j] != "{":
            continue
        depth = 0
        k = j
        while k < len(code):
            if code[k] == "{":
                depth += 1
            elif code[k] == "}":
                depth -= 1
                if depth == 0:
                    break
            k += 1
        # blank m.start()..k (keep newlines)
        for p in range(m.start(), min(k + 1, len(code))):
            if result[p] != "\n":
                result[p] = " "
    return "".join(result)


def scan_text(src: str) -> list[int]:
    """Return 1-based line numbers where a forbidden register_parquet( call
    survives comment/literal/cfg(test) stripping."""
    cleaned = remove_cfg_test_blocks(strip_comments_and_literals(src))
    hits = []
    for idx, line in enumerate(cleaned.splitlines(), start=1):
        if NEEDLE in line:
            hits.append(idx)
    return hits


def scan_repo(root: Path) -> list[tuple[Path, int]]:
    violations: list[tuple[Path, int]] = []
    for src_dir in sorted(root.glob("crates/*/src")):
        for rs in sorted(src_dir.rglob("*.rs")):
            text = rs.read_text(encoding="utf-8", errors="replace")
            if "register_parquet(" not in text:
                continue
            for ln in scan_text(text):
                violations.append((rs.relative_to(root), ln))
    return violations


# --------------------------------------------------------------------------
SELF_TESTS = [
    # (name, source, expected_violation_count)
    ("runtime call is caught",
     'fn build(ctx: &SessionContext) {\n    ctx.register_parquet("t", p, d).await?;\n}\n', 1),
    ("cfg(test) module is allowed",
     '#[cfg(test)]\nmod tests {\n    fn t() { ctx.register_parquet("t", p, d); }\n}\n', 0),
    ("doc comment is allowed",
     '/// unlike `register_parquet(...)` this uses the fast reader\nfn f() {}\n', 0),
    ("line comment is allowed",
     'fn f() {\n    // was ctx.register_parquet(...) before\n    ok();\n}\n', 0),
    ("string literal is allowed",
     'fn f() {\n    let s = "call register_parquet( here";\n}\n', 0),
    ("cfg(test) fn is allowed",
     '#[cfg(test)]\nfn helper() {\n    ctx.register_parquet("t", p, d);\n}\nfn shipped() { ok(); }\n', 0),
    ("runtime after a test module still caught",
     '#[cfg(test)]\nmod tests { fn t() { register_parquet("a",1,2); } }\n'
     'pub fn run() { ctx.register_parquet("b", p, d); }\n', 1),
    ("block comment is allowed",
     'fn f() {\n    /* ctx.register_parquet("t", p, d); */\n    ok();\n}\n', 0),
]


def self_test() -> int:
    failures = 0
    for name, src, expected in SELF_TESTS:
        got = len(scan_text(src))
        ok = got == expected
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}: expected {expected}, got {got}")
        if not ok:
            failures += 1
    if failures:
        print(f"self-test: {failures} FAILED")
        return 1
    print(f"self-test: all {len(SELF_TESTS)} passed")
    return 0


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        return self_test()
    root = Path(".")
    if "--root" in argv:
        root = Path(argv[argv.index("--root") + 1])
    violations = scan_repo(root)
    if violations:
        print("FORBIDDEN: arrow-rs `register_parquet(` in shipped runtime code "
              "(use EmatixFastParquetTableProvider instead):", file=sys.stderr)
        for path, ln in violations:
            print(f"  {path}:{ln}", file=sys.stderr)
        print("\nIf this is genuinely test-only, move it inside a #[cfg(test)] "
              "module. ematix-parquet must be the only shipped scan path.",
              file=sys.stderr)
        return 1
    print("OK: no arrow-rs register_parquet in shipped runtime code.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
