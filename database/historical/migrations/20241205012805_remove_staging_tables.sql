--- Add migration script here

-- Step 3: Drop the `staging_bid_ask` table and its index, as it is no longer needed
DROP INDEX IF EXISTS idx_staging_bid_ask_mbp_id_depth;
DROP INDEX IF EXISTS idx_staging_mbp_instrument_ts_event; 
DROP TABLE IF EXISTS staging_bid_ask CASCADE;
DROP TABLE IF EXISTS staging_mbp CASCADE;

