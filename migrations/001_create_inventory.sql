CREATE TABLE inventory (
    store_id   text        NOT NULL,
    sku        text        NOT NULL,
    on_hand    integer     NOT NULL DEFAULT 0,
    reserved   integer     NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (store_id, sku),
    CONSTRAINT inventory_on_hand_nonneg     CHECK (on_hand >= 0),
    CONSTRAINT inventory_reserved_nonneg    CHECK (reserved >= 0),
    CONSTRAINT inventory_reserved_le_onhand CHECK (reserved <= on_hand)
);
