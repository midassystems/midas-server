-- Continuous Volume 
CREATE MATERIALIZED VIEW futures_continuous_volume_windows AS
WITH ordered_data AS (
    SELECT
        m.instrument_id,
        m.ts_recv,
        m.size
    FROM futures_mbp m
    INNER JOIN futures i ON m.instrument_id = i.instrument_id
    WHERE m.action = 84  -- Filter only trades
),
aggregated_data AS (
    SELECT
        instrument_id,
        floor(ts_recv / 86400000000000) * 86400000000000 AS ts_event, -- Maintain nanoseconds
        SUM(size) AS volume
    FROM ordered_data
    GROUP BY
        instrument_id,
        floor(ts_recv / 86400000000000) * 86400000000000
),
daily_volumes AS (
    SELECT 
        i.ticker, 
        a.instrument_id,
        CAST(a.ts_event AS BIGINT) AS ts_event, -- Explicitly cast to BIGINT to avoid scientific notation
        a.volume AS daily_volume, -- Keep daily volume for each day
        i.expiration_date 
    FROM aggregated_data a
    INNER JOIN futures i ON a.instrument_id = i.instrument_id
),
ranked_volumes AS (
    SELECT 
        ticker,
        ts_event,
        instrument_id,
        daily_volume,
        expiration_date,
        RANK() OVER (PARTITION BY LEFT(ticker, 2), ts_event ORDER BY daily_volume DESC) - 1 AS rank 
    FROM daily_volumes
),
start_end_schedule AS (
    SELECT 
        rv.ticker,
        rv.instrument_id,
        -- Start time is the beginning of the next day after becoming the volume leader
        rv.ts_event + (86400000000000) AS start_time,  -- Add 1 day in nanoseconds
        -- End time is the end of the day when the contract loses its rank
        COALESCE(
            LEAD(rv.ts_event + 86400000000000) OVER (PARTITION BY LEFT(rv.ticker, 2), rv.rank ORDER BY rv.ts_event),
            CEIL(rv.expiration_date / 86400000000000.0) * 86400000000000
            -- rv.expiration_date
        ) - 1 AS end_time, 
        rv.daily_volume,
        rv.rank
    FROM ranked_volumes rv
),
rolling_schedule AS (
    SELECT
        ticker,
        instrument_id,
        rank,
        MIN(start_time) AS start_time,
        MAX(end_time) AS end_time
    FROM start_end_schedule
    GROUP BY ticker, instrument_id, rank
)
SELECT * FROM rolling_schedule
ORDER BY ticker, rank;

-- Continuous Calender
CREATE MATERIALIZED VIEW futures_continuous_calendar_windows AS
  WITH raw_data AS (
      SELECT
          ticker,
          instrument_id,
          expiration_date,
          ROW_NUMBER() OVER (
              PARTITION BY LEFT(ticker, 2)
              ORDER BY expiration_date
          ) - 1 AS rank
      FROM futures
  ),
  ts_bounds AS (
      SELECT 
          MIN(ts_recv) AS min_ts_recv, 
          MAX(ts_recv) AS max_ts_recv 
      FROM futures_mbp
  ),
  rank_data AS (
      SELECT
          rw.ticker,
          rw.instrument_id,
          rw.rank,
          COALESCE(
              LAG(
                  CEIL(expiration_date / 86400000000000.0) * 86400000000000
              ) OVER (PARTITION BY LEFT(rw.ticker, 2) ORDER BY rw.rank),
              FLOOR(tb.min_ts_recv / 86400000000000.0) * 86400000000000
          ) AS start_time,
          CEIL(expiration_date / 86400000000000.0) * 86400000000000 -1 
          AS end_time
      FROM raw_data rw
      CROSS JOIN ts_bounds tb
  ),
  adjusted_rank_data AS (
      SELECT
          ticker,
          instrument_id,
          rank,
          start_time,
          end_time,
          LEAD(start_time) OVER (PARTITION BY LEFT(ticker, 2) ORDER BY rank) AS next_start_time
      FROM rank_data
  )
  SELECT
      ticker,
      instrument_id,
      rank,
      start_time,
      end_time
  FROM adjusted_rank_data
  ORDER BY LEFT(ticker, 2), rank;


