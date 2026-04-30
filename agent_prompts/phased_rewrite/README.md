# Phased Rewrite Prompt Pack

Use these prompt files literally.

Execution order:
1. phase-01-implementation.md
2. phase-01-review-a-architecture-race.md
3. phase-01-review-b-regression-integration.md
4. merge to `develop`
5. continue with phase 02, etc.

Global rules:
- Base branch is always latest `develop`
- One phase per branch / PR
- 2 AI reviews required before merge
- Reviewer identity is not pinned; any 2 automatic AI agents are fine as long as they use the two different review prompts for that phase
- WSS heartbeat is 5 seconds
- Do not mix scope across phases
- Do not change logging shape unless explicitly required
- Use Decimal for money/price/size logic
