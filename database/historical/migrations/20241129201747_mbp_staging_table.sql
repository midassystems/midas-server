-- Add migration script here

CREATE TABLE IF NOT EXISTS staging_mbp (
  id SERIAL PRIMARY KEY,
  instrument_id INTEGER NOT NULL, -- corresponds to uint32_t
  ts_event BIGINT NOT NULL, -- corresponds to uint64_t, stored as TIMESTAMP in QuestDB
  price BIGINT NOT NULL, -- corresponds to float64
  size INTEGER NOT NULL, -- corresponds to uint32_t
  action INTEGER NOT NULL, -- corresponds to char
  side INTEGER NOT NULL, -- corresponds to char
  flags INTEGER NOT NULL,
  ts_recv BIGINT NOT NULL, -- corresponds to uint64_t, stored as TIMESTAMP in QuestDB
  ts_in_delta INTEGER NOT NULL, -- corresponds to int32_t
  sequence INTEGER NOT NULL, -- corresponds to uint32_t
  order_book_hash VARCHAR NOT NULL,
  CONSTRAINT fk_instrument_staging_mbp
    FOREIGN KEY(instrument_id) 
      REFERENCES instrument(id)
      ON DELETE CASCADE,
  CONSTRAINT staging_unique_instrument_ts_sequence_event UNIQUE (instrument_id, ts_event, price, size, flags, sequence, order_book_hash, ts_recv, action, side)
);

CREATE TABLE IF NOT EXISTS staging_bid_ask (
  id SERIAL PRIMARY KEY,
  mbp_id INTEGER NOT NULL, -- Foreign key to mbp_1
  depth INTEGER NOT NULL, -- Depth level in the order book
  bid_px BIGINT NOT NULL, -- Bid price
  bid_sz INTEGER NOT NULL, -- Bid size
  bid_ct INTEGER NOT NULL, -- Bid order count
  ask_px BIGINT NOT NULL, -- Ask price
  ask_sz INTEGER NOT NULL, -- Ask size
  ask_ct INTEGER NOT NULL, -- Ask order count
  CONSTRAINT fk_staging_mbp_bid_ask
    FOREIGN KEY(mbp_id) 
      REFERENCES staging_mbp(id)
      ON DELETE CASCADE
);

-- Create the indexes
CREATE INDEX idx_staging_mbp_instrument_ts_event ON staging_mbp (instrument_id, ts_event);
CREATE INDEX idx_staging_bid_ask_mbp_id_depth ON staging_bid_ask (mbp_id, depth);


