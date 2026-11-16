create table audit_log (
    id              bigserial primary key,
    actor           text        not null,
    role            text        not null,
    action          text        not null,
    target          text        not null default '',
    requested_at    timestamptz not null,
    finished_at     timestamptz not null default now(),
    result          text        not null,
    error           text        not null default '',
    idempotency_key text        not null default '',
    details         jsonb       not null default '{}'
);

create table alerts (
    id          bigserial primary key,
    rule        text        not null,
    severity    text        not null,
    state       text        not null check (state in ('firing', 'acknowledged', 'resolved')),
    summary     text        not null,
    details     jsonb       not null default '{}',
    fired_at    timestamptz not null,
    acked_at    timestamptz,
    acked_by    text        not null default '',
    resolved_at timestamptz,
    updated_at  timestamptz not null default now()
);
create index alerts_state_idx on alerts (state, id desc);
-- at most one live alert per rule: the database enforces the dedup the engine relies on
create unique index alerts_one_live_per_rule on alerts (rule) where state in ('firing', 'acknowledged');

create table devices (
    token        text primary key,
    user_login   text        not null,
    platform     text        not null default '',
    created_at   timestamptz not null default now(),
    last_seen_at timestamptz not null default now()
);

create table idempotency_keys (
    actor        text        not null,
    key          text        not null,
    route        text        not null,
    request_hash text        not null,
    status       int         not null default 0, -- 0 = in progress
    response     bytea,
    created_at   timestamptz not null default now(),
    primary key (actor, key)
);
