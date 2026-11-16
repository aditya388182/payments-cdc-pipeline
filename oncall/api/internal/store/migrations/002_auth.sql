-- Authorization-code flow state. Only hashes of secrets are stored (state, one-time code, refresh token).
create table oauth_states (
    state_hash     text primary key,
    code_challenge text        not null,
    redirect_uri   text        not null,
    expires_at     timestamptz not null
);

create table auth_codes (
    code_hash      text primary key,
    login          text        not null,
    role           text        not null,
    code_challenge text        not null,
    redirect_uri   text        not null,
    expires_at     timestamptz not null,
    used_at        timestamptz
);

create table refresh_tokens (
    id          uuid primary key default gen_random_uuid(),
    family_id   uuid        not null,
    login       text        not null,
    role        text        not null,
    token_hash  text        not null unique,
    created_at  timestamptz not null default now(),
    expires_at  timestamptz not null,
    used_at     timestamptz,  -- set when rotated; presenting it again is reuse
    revoked_at  timestamptz
);
create index refresh_tokens_family_idx on refresh_tokens (family_id);
