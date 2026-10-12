-- Task 21: cross-process account execution lease.
--
-- Additive table only: one row per copier account, guarding that at most
-- ONE process is the active writer for that account at any time. The
-- account identity is the persisted user_id string (see
-- domain.account_identity.CopierAccountId); it is NOT keyed by wallet or
-- display name, so identity is stable across config renames and
-- deposit-wallet re-binding.
--
-- One lease per account regardless of trading mode: paper and live both
-- write money-shaped state (reservations, orders, positions) to this
-- database, so a paper process and a live process configured for the same
-- account contend for the SAME row. The holder's mode is recorded for
-- operator observability only.
--
-- holder_host / holder_token   holder identity (hostname + per-process uuid)
-- acquired_at                   when the current holder won the lease
-- renewed_at                    last successful heartbeat
-- expires_at                    TTL deadline; past it, a new process may take
--                               over (logged as a takeover)
-- released_at                   set on graceful shutdown; a released row is
--                               free for immediate clean acquire
CREATE TABLE IF NOT EXISTS account_execution_leases (
    user_id      text PRIMARY KEY,
    holder_host  text NOT NULL,
    holder_token text NOT NULL,
    mode         text NOT NULL DEFAULT '',
    acquired_at  timestamptz NOT NULL DEFAULT now(),
    renewed_at   timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz NOT NULL,
    released_at  timestamptz
)
