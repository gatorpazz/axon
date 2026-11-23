-- Event-sourced job storage with row-level security.
--
-- `job_events` is the append-only source of truth. `jobs` is a projection kept
-- in the same transaction as each append; its `last_seq` column doubles as the
-- optimistic-concurrency guard (UPDATE ... WHERE last_seq = $expected).
--
-- Tenant isolation is enforced here, not in application code: every
-- transaction declares who it acts for with
--     SELECT set_config('axon.actor', '<principal id>', true);
-- and the policies below hide every row that actor does not own. A connection
-- that never sets the actor sees nothing.

CREATE TABLE jobs (
    id         uuid        PRIMARY KEY,
    owner      text        NOT NULL,
    status     text        NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    last_seq   bigint      NOT NULL CHECK (last_seq >= 1)
);

CREATE INDEX jobs_owner_idx ON jobs (owner, id DESC);
CREATE INDEX jobs_unfinished_idx ON jobs (id) WHERE status IN ('queued', 'running');

CREATE TABLE job_events (
    job_id uuid        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    seq    bigint      NOT NULL CHECK (seq >= 1),
    at     timestamptz NOT NULL,
    kind   jsonb       NOT NULL,
    PRIMARY KEY (job_id, seq)
);

-- The acting principal for the current transaction, or NULL if undeclared.
-- Principal ids always contain a colon, so none can equal 'system'.
CREATE FUNCTION axon_actor() RETURNS text
    LANGUAGE sql STABLE
    AS $$ SELECT nullif(current_setting('axon.actor', true), '') $$;

ALTER TABLE jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE jobs FORCE ROW LEVEL SECURITY; -- applies to the table owner too

CREATE POLICY jobs_actor ON jobs
    USING (owner = axon_actor() OR axon_actor() = 'system')
    WITH CHECK (owner = axon_actor() OR axon_actor() = 'system');

ALTER TABLE job_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE job_events FORCE ROW LEVEL SECURITY;

-- An event is visible exactly when its job is. The subquery is itself subject
-- to the policy on `jobs`, so there is a single definition of "may see".
CREATE POLICY job_events_actor ON job_events
    USING (EXISTS (SELECT 1 FROM jobs WHERE jobs.id = job_events.job_id))
    WITH CHECK (EXISTS (SELECT 1 FROM jobs WHERE jobs.id = job_events.job_id));
