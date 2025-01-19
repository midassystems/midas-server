-- Single source of truth for all instruments
CREATE TABLE IF NOT EXISTS instrument (
  id SERIAL PRIMARY KEY,
  dataset SMALLINT NOT NULL, 
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Futures - 1
CREATE TABLE IF NOT EXISTS futures (
  id SERIAL PRIMARY KEY,
  instrument_id INTEGER NOT NULL UNIQUE, -- corresponds to uint32_t
  ticker VARCHAR(10) NOT NULL UNIQUE,
  name VARCHAR(25) NOT NULL,
  dataset SMALLINT NOT NULL DEFAULT 1, -- Always 1 for futures 
  vendor VARCHAR(50) NOT NULL,
  vendor_data BIGINT NOT NULL,
  last_available BIGINT NOT NULL,
  first_available BIGINT NOT NULL,
  active BOOL NOT NULL,
  CONSTRAINT fk_instrument_futures
    FOREIGN KEY(instrument_id) 
      REFERENCES instrument(id)
      ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS futures_mbp (
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
  discriminator INTEGER NOT NULL,
  order_book_hash VARCHAR NOT NULL,
  CONSTRAINT fk_instrument_futures_mbp
    FOREIGN KEY(instrument_id) 
      REFERENCES futures(instrument_id)
      ON DELETE CASCADE,
  CONSTRAINT unique_futures_instrument_ts_sequence_event UNIQUE (instrument_id, ts_event, price, size, flags, sequence, order_book_hash, ts_recv, action, side, discriminator)
);



CREATE TABLE IF NOT EXISTS futures_bid_ask (
  id SERIAL PRIMARY KEY,
  mbp_id INTEGER NOT NULL, -- Foreign key to mbp_1
  depth INTEGER NOT NULL, -- Depth level in the order book
  bid_px BIGINT NOT NULL, -- Bid price
  bid_sz INTEGER NOT NULL, -- Bid size
  bid_ct INTEGER NOT NULL, -- Bid order count
  ask_px BIGINT NOT NULL, -- Ask price
  ask_sz INTEGER NOT NULL, -- Ask size
  ask_ct INTEGER NOT NULL, -- Ask order count
  CONSTRAINT fk_futures_bid_ask
    FOREIGN KEY(mbp_id) 
      REFERENCES futures_mbp(id)
      ON DELETE CASCADE
);

-- Create the indexes
CREATE INDEX idx_futures_mbp_instrument_ts_event ON futures_mbp (instrument_id, ts_event);
CREATE INDEX idx_futures_bid_ask_mbp_id_depth ON futures_bid_ask (mbp_id, depth);

-- Equities - 2
CREATE TABLE IF NOT EXISTS equities (
  id SERIAL PRIMARY KEY,
  instrument_id INTEGER NOT NULL UNIQUE, -- corresponds to uint32_t
  ticker VARCHAR(10) NOT NULL UNIQUE,
  name VARCHAR(25) NOT NULL,
  dataset SMALLINT NOT NULL DEFAULT 2, -- Always 1 for futures 
  vendor VARCHAR(50) NOT NULL,
  vendor_data BIGINT NOT NULL,
  last_available BIGINT NOT NULL,
  first_available BIGINT NOT NULL,
  active BOOL NOT NULL,
  CONSTRAINT fk_instrument_equities
    FOREIGN KEY(instrument_id) 
      REFERENCES instrument(id)
      ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS equities_mbp (
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
  discriminator INTEGER NOT NULL,
  order_book_hash VARCHAR NOT NULL,
  CONSTRAINT fk_instrument_equities_mbp
    FOREIGN KEY(instrument_id) 
      REFERENCES equities(instrument_id)
      ON DELETE CASCADE,
  CONSTRAINT unique_equities_instrument_ts_sequence_event UNIQUE (instrument_id, ts_event, price, size, flags, sequence, order_book_hash, ts_recv, action, side, discriminator)
);



CREATE TABLE IF NOT EXISTS equities_bid_ask (
  id SERIAL PRIMARY KEY,
  mbp_id INTEGER NOT NULL, -- Foreign key to mbp_1
  depth INTEGER NOT NULL, -- Depth level in the order book
  bid_px BIGINT NOT NULL, -- Bid price
  bid_sz INTEGER NOT NULL, -- Bid size
  bid_ct INTEGER NOT NULL, -- Bid order count
  ask_px BIGINT NOT NULL, -- Ask price
  ask_sz INTEGER NOT NULL, -- Ask size
  ask_ct INTEGER NOT NULL, -- Ask order count
  CONSTRAINT fk_equities_bid_ask
    FOREIGN KEY(mbp_id) 
      REFERENCES equities_mbp(id)
      ON DELETE CASCADE
);

-- Create the indexes
CREATE INDEX idx_equities_mbp_instrument_ts_event ON equities_mbp (instrument_id, ts_event);
CREATE INDEX idx_equities_bid_ask_mbp_id_depth ON equities_bid_ask (mbp_id, depth);

-- Option - 3
CREATE TABLE IF NOT EXISTS option (
  id SERIAL PRIMARY KEY,
  instrument_id INTEGER NOT NULL UNIQUE, -- corresponds to uint32_t
  ticker VARCHAR(10) NOT NULL UNIQUE,
  name VARCHAR(25) NOT NULL,
  dataset SMALLINT NOT NULL DEFAULT 3, -- Always 1 for futures 
  vendor VARCHAR(50) NOT NULL,
  vendor_data BIGINT NOT NULL,
  last_available BIGINT NOT NULL,
  first_available BIGINT NOT NULL,
  active BOOL NOT NULL,
  CONSTRAINT fk_instrument_option
    FOREIGN KEY(instrument_id) 
      REFERENCES instrument(id)
      ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS option_mbp (
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
  discriminator INTEGER NOT NULL,
  order_book_hash VARCHAR NOT NULL,
  CONSTRAINT fk_instrument_option_mbp
    FOREIGN KEY(instrument_id) 
      REFERENCES option(instrument_id)
      ON DELETE CASCADE,
  CONSTRAINT unique_option_instrument_ts_sequence_event UNIQUE (instrument_id, ts_event, price, size, flags, sequence, order_book_hash, ts_recv, action, side, discriminator)
);



CREATE TABLE IF NOT EXISTS option_bid_ask (
  id SERIAL PRIMARY KEY,
  mbp_id INTEGER NOT NULL, -- Foreign key to mbp_1
  depth INTEGER NOT NULL, -- Depth level in the order book
  bid_px BIGINT NOT NULL, -- Bid price
  bid_sz INTEGER NOT NULL, -- Bid size
  bid_ct INTEGER NOT NULL, -- Bid order count
  ask_px BIGINT NOT NULL, -- Ask price
  ask_sz INTEGER NOT NULL, -- Ask size
  ask_ct INTEGER NOT NULL, -- Ask order count
  CONSTRAINT fk_option_bid_ask
    FOREIGN KEY(mbp_id) 
      REFERENCES option_mbp(id)
      ON DELETE CASCADE
);

-- Create the indexes
CREATE INDEX idx_option_mbp_instrument_ts_event ON option_mbp (instrument_id, ts_event);
CREATE INDEX idx_option_bid_ask_mbp_id_depth ON option_bid_ask (mbp_id, depth);


-- Function for Bbo queries 
CREATE FUNCTION coalesce_r_sfunc(state anyelement, value anyelement)
RETURNS anyelement
IMMUTABLE PARALLEL SAFE
AS $$
    SELECT COALESCE(value, state);
$$ LANGUAGE sql;

CREATE AGGREGATE find_last_ignore_nulls(anyelement) (
    SFUNC = coalesce_r_sfunc,
    STYPE = anyelement
);
