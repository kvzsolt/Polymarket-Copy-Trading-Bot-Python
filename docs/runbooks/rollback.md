# Code Rollback

1. Pause new venue submissions and verify the returned generation:

   ```bash
   pmcopy trading pause --reason "code rollback"
   pmcopy trading status
   ```

2. Preserve logs and database state. Do not delete or re-submit ambiguous
   orders.
3. Deploy the recorded previous-good Git commit. The `trading_control`
   migration is additive and remains compatible with the previous application.
4. Run startup reconciliation, `pmcopy doctor --live`, drift checks, and the
   release gate while trading remains paused.
5. Start the process and observe readiness.
6. Resume only after an explicit operator decision:

   ```bash
   pmcopy trading resume --confirm LIVE --reason "rollback validated"
   ```

`scripts/rollback.sh <commit>` automates the pause, artifact snapshot, checkout,
doctor, and smoke phases. It never resumes trading.
