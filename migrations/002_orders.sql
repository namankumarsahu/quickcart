-- Milestone 2: orders, order items, idempotency keys.
-- NOTE: docker-entrypoint-initdb.d only runs on a FRESH data volume.
-- To apply this to an existing volume: see README.md.

CREATE TABLE orders (
    -- uuid: ids are unguessable (no /orders/1, /orders/2 enumeration later).
    id         uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    store_id   text        NOT NULL,
    status     text        NOT NULL DEFAULT 'created',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),

    -- Only the seven known states can ever be stored, whatever the app does.
    CONSTRAINT orders_status_valid CHECK (status IN
        ('created','reserved','confirmed','packed','delivered','cancelled','expired'))
);

-- Milestone 3's expiry worker will scan "reserved orders older than the TTL";
-- this index makes that scan cheap.
CREATE INDEX orders_status_created_idx ON orders (status, created_at);

CREATE TABLE order_items (
    -- ON DELETE CASCADE: items never outlive their order.
    order_id uuid    NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    sku      text    NOT NULL,
    quantity integer NOT NULL,

    -- One line per SKU per order (the PK also indexes lookups by order_id).
    PRIMARY KEY (order_id, sku),
    -- Zero or negative quantities are nonsense; the DB refuses them.
    CONSTRAINT order_items_quantity_positive CHECK (quantity > 0)
);

CREATE TABLE idempotency_keys (
    -- PRIMARY KEY = unique index. THIS is what makes two simultaneous requests
    -- with the same key collide safely: the database serialises them.
    key             text        PRIMARY KEY,
    request_hash    text        NOT NULL,
    -- Filled in at the end of the same transaction that creates the order.
    response_status integer,
    response_body   json,
    order_id        uuid        REFERENCES orders (id),
    created_at      timestamptz NOT NULL DEFAULT now(),

    -- A stored response is either complete or absent, never half-written.
    CONSTRAINT idempotency_response_complete
        CHECK ((response_status IS NULL) = (response_body IS NULL))
);

-- Second line of defence for the state machine. Go code checks transitions
-- first (clear errors); this trigger makes illegal jumps impossible even for
-- a buggy query or a person with psql.
CREATE FUNCTION orders_check_transition() RETURNS trigger AS $$
BEGIN
    IF NEW.status = OLD.status THEN
        RETURN NEW;  -- no-op update, harmless
    END IF;
    IF (OLD.status, NEW.status) IN (
        ('created',   'reserved'),
        ('reserved',  'confirmed'),
        ('confirmed', 'packed'),
        ('packed',    'delivered'),
        ('created',   'cancelled'),
        ('reserved',  'cancelled'),
        ('confirmed', 'cancelled'),
        ('reserved',  'expired')
    ) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'illegal order status transition % -> %', OLD.status, NEW.status
        USING ERRCODE = 'check_violation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER orders_status_transition
    BEFORE UPDATE OF status ON orders
    FOR EACH ROW EXECUTE FUNCTION orders_check_transition();
