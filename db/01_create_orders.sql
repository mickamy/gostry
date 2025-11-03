\connect gostry

CREATE TABLE orders
(
    id          UUID PRIMARY KEY,
    customer_id UUID           NOT NULL,
    amount      NUMERIC(10, 2) NOT NULL,
    status      TEXT           NOT NULL,
    updated_at  TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE TABLE orders_history
(
    history_id  SERIAL PRIMARY KEY,
    id          UUID,
    operation   TEXT        NOT NULL,
    operated_at TIMESTAMPTZ NOT NULL,
    operated_by TEXT,
    trace_id    TEXT,
    reason      TEXT,
    before      JSONB,
    after       JSONB
);
