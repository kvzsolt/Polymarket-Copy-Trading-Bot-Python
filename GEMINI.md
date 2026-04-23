# Gemini Instructions for Polymarket-Copy-Trading-Bot-Python

Start every task by reading these files if they exist:
- `/home/anon/.openclaw/workspace/contexts/polymarket-copy-trading-bot-python/FULL_CONTEXT.md`
- `/home/anon/.openclaw/workspace/contexts/polymarket-copy-trading-bot-python/IMPLEMENTATION_PLAN.md`
- `/home/anon/.openclaw/workspace/contexts/polymarket-copy-trading-bot-python/WORKING_SOLUTIONS.md`
- `/home/anon/.openclaw/workspace/contexts/polymarket-copy-trading-bot-python/FAILURES.md`
- `LEARNINGS.md`

Minimum required behavior:
- Read the context files before changing code.
- Summarize the current learnings/context in 3 to 5 bullets before implementation.
- Stay on the active copy-trading lane only.
- Keep changes small, concrete, and reviewable.
- Do not auto-commit.
- Do not auto-push.
- Do not open a PR by yourself unless explicitly asked.
- Run focused validation after each task.
- Update `LEARNINGS.md` when you discover a durable project-specific lesson.

Required workflow for this repo:
- Work task by task on the active branch.
- Finish the assigned branch/task scope before PR creation.
- Before any PR, run an Opus 4.7 / Claude Code review gate on the completed branch scope.
- Apply the review fixes on the same branch.
- When the branch is fully PR-ready, run the final pre-PR Opus 4.7 / Claude Code gate.
- Only then: PR -> Qodo review -> Qodo fixes -> merge.

Fail closed:
- If context files disagree with the current branch/task, stop and report the mismatch.
- If the working tree is dirty with unrelated changes, do not mix them into your task.
- Do not rely on live venue state for tests when stable mocks are required.
