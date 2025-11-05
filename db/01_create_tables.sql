\connect gostry

CREATE TABLE orders
(
    id          UUID PRIMARY KEY,
    customer_id UUID           NOT NULL,
    amount      NUMERIC(10, 2) NOT NULL,
    status      TEXT           NOT NULL,
    updated_at  TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE TABLE order_items
(
    order_item_id SERIAL PRIMARY KEY,
    order_id      UUID           NOT NULL,
    sku           TEXT           NOT NULL,
    quantity      INTEGER        NOT NULL,
    unit_price    NUMERIC(10, 2) NOT NULL,
    created_at    TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE TABLE payments
(
    payment_id SERIAL PRIMARY KEY,
    order_id   UUID           NOT NULL,
    amount     NUMERIC(10, 2) NOT NULL,
    method     TEXT           NOT NULL,
    paid_at    TIMESTAMPTZ    NOT NULL DEFAULT now()
);
