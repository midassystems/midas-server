-- Add migration script here
CREATE TABLE IF NOT EXISTS instrument (
  id SERIAL PRIMARY KEY,
  ticker VARCHAR(10) NOT NULL UNIQUE,
  name VARCHAR(25) NOT NULL
);

CREATE TABLE IF NOT EXISTS mbp (
  id SERIAL PRIMARY KEY,
  -- publisher_id INTEGER, -- corresponds to uint16_t
  instrument_id INTEGER, -- corresponds to uint32_t
  ts_event BIGINT, -- corresponds to uint64_t, stored as TIMESTAMP in QuestDB
  price BIGINT, -- corresponds to float64
  size INTEGER, -- corresponds to uint32_t
  action INTEGER, -- corresponds to char
  side INTEGER, -- corresponds to char
  -- flags INTEGER, -- corresponds to uint8_t DELETE
  ts_recv BIGINT, -- corresponds to uint64_t, stored as TIMESTAMP in QuestDB
  ts_in_delta INTEGER, -- corresponds to int32_t
  sequence INTEGER, -- corresponds to uint32_t
  CONSTRAINT fk_instrument_mbp
    FOREIGN KEY(instrument_id) 
      REFERENCES instrument(id)
      ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS bid_ask (
  id SERIAL PRIMARY KEY,
  mbp_id INTEGER NOT NULL, -- Foreign key to mbp_1
  depth INTEGER NOT NULL, -- Depth level in the order book
  bid_px BIGINT NOT NULL, -- Bid price
  bid_sz INTEGER NOT NULL, -- Bid size
  bid_ct INTEGER NOT NULL, -- Bid order count
  ask_px BIGINT NOT NULL, -- Ask price
  ask_sz INTEGER NOT NULL, -- Ask size
  ask_ct INTEGER NOT NULL, -- Ask order count
  CONSTRAINT fk_mbp_bid_ask
    FOREIGN KEY(mbp_id) 
      REFERENCES mbp(id)
      ON DELETE CASCADE
);

