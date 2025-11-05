\connect gostry

CREATE TABLE orders
(
    id          UUID PRIMARY KEY,
    customer_id UUID           NOT NULL,
    amount      NUMERIC(10, 2) NOT NULL,
    status      TEXT           NOT NULL,
    updated_at  TIMESTAMPTZ    NOT NULL DEFAULT now()
);
