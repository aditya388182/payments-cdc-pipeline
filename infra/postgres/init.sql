-- 1. Primary transactions table
CREATE TABLE transactions (
    transaction_id  UUID PRIMARY KEY,
    merchant_id     VARCHAR(50) NOT NULL,
    amount_minor    BIGINT NOT NULL,
    currency        CHAR(3) NOT NULL,
    status          VARCHAR(20) NOT NULL,
    event_type      VARCHAR(20) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2. Merchant dimension table
CREATE TABLE merchants (
    merchant_id   VARCHAR(50) PRIMARY KEY,
    merchant_name VARCHAR(200) NOT NULL
);

-- 3. Heartbeat table (prevents WAL bloat during low activity)
CREATE TABLE debezium_heartbeat (
    id        INT PRIMARY KEY DEFAULT 1,
    last_beat TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO debezium_heartbeat VALUES (1, now());

-- 4. Seed 20 merchants
INSERT INTO merchants
SELECT 'MERCH_' || lpad(g::text, 3, '0'), 'Merchant ' || g
FROM generate_series(1, 20) g;

-- 5. Seed 1,000 transactions
INSERT INTO transactions (transaction_id, merchant_id, amount_minor, currency, status, event_type)
SELECT 
    gen_random_uuid(),
    'MERCH_' || lpad((1 + floor(random()*20))::text, 3, '0'),
    (100 + floor(random()*99900))::bigint,
    (ARRAY['USD','EUR','GBP','JPY'])[1 + floor(random()*4)],
    (ARRAY['PENDING','SETTLED','FAILED'])[1 + floor(random()*3)],
    (ARRAY['AUTHORIZATION','CAPTURE','REFUND','CHARGEBACK'])[1 + floor(random()*4)]
FROM generate_series(1, 1000);