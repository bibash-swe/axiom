# Axiom Engineering Standards

## 1. Documentation & The "Pointer Pattern"
Code documentation explains **behavior**. Types and schemas define **shape**. ADRs define **why**.
This applies across all languages in this repository (Python, SQL, YAML, etc.).

* **No Redundancy:** Do not explain standard syntax. If a docstring simply restates the function signature, delete it.
* **Behavior First:** Docstrings must call out side effects, database locks, idempotency guarantees, and failure modes. 
* **The Pointer Pattern:** Do not write architectural reasoning inline. Inline comments and docstrings should be terse and point to `docs/decisions.md` (e.g., `see docs/decisions.md #6`) for the underlying justification.

## 2. Git Workflow & History
* **Atomic Commits:** Commits should be logically separated (e.g., schema, contracts, implementation).
* **Conventional Commits:** Use standard prefixes (`feat:`, `chore:`, `fix:`, `docs:`).
* **Squash on Merge:** Feature branches can contain iterative fixup commits. When merging via Pull Request to `main`, commits must be squashed to ensure the `main` branch maintains a pristine, intent-driven history.