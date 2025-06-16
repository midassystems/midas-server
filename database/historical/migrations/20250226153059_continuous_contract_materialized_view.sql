-- Added is_continuous to instruments 
ALTER TABLE futures
ADD COLUMN is_continuous boolean DEFAULT False;

ALTER TABLE equities
ADD COLUMN is_continuous boolean DEFAULT False;

ALTER TABLE option
ADD COLUMN is_continuous boolean DEFAULT False;



CREATE MATERIALIZED VIEW futures_continuous AS
  -- calendar contracts 
  WITH ts_bounds AS (
      SELECT 
          MIN(ts_recv) AS min_ts_recv, 
          MAX(ts_recv) AS max_ts_recv 
      FROM futures_mbp
  ),
  ts_days AS (
      SELECT DISTINCT
          FLOOR(ts_recv / 86400000000000.0) * 86400000000000 AS ts_day
      FROM futures_mbp
  ),
  raw_data AS (
      SELECT
          ticker,
          instrument_id,
          expiration_date
      FROM futures
      WHERE is_continuous = false
  ),
  crossed AS (
      SELECT
          td.ts_day,
          rd.ticker,
          rd.instrument_id,
          rd.expiration_date,
          rd.expiration_date - td.ts_day AS distance
      FROM ts_days td
      CROSS JOIN raw_data rd
      WHERE rd.expiration_date >= td.ts_day -- 
  ),
  ranked AS (
      SELECT
          cd.ts_day,
          cd.ticker, 
          cd.instrument_id,       
          ROW_NUMBER() OVER (
              PARTITION BY ts_day, LEFT(ticker, 2)
              ORDER BY distance, expiration_date
          ) - 1 AS rank
      FROM crossed cd
  ), 
  c_continuous_ranked AS (
      SELECT 
        r.ts_day, 
        LEFT(ticker, 2) || '.c.' || r.rank AS continuous_ticker, 
        r.instrument_id
      FROM ranked r 
  ), 
  c_continuous_lookup AS (
      SELECT
          ticker AS continuous_ticker,
          instrument_id AS continuous_instrument_id
      FROM futures
      WHERE is_continuous = true
  ), 
  c_continuous AS (
    SELECT
        cr.ts_day,
        cr.continuous_ticker,
        cr.instrument_id,
        cl.continuous_instrument_id
    FROM c_continuous_ranked cr
    LEFT JOIN c_continuous_lookup cl
      ON cr.continuous_ticker = cl.continuous_ticker
  ), 
  calendar_rollovers AS (
    SELECT
      *,
      CASE
        WHEN LAG(instrument_id) OVER (PARTITION BY continuous_ticker ORDER BY ts_day) IS NULL THEN 0
        WHEN LAG(instrument_id) OVER (PARTITION BY continuous_ticker ORDER BY ts_day) IS DISTINCT FROM instrument_id THEN 1
        ELSE 0
      END AS rollover_flag   
  FROM c_continuous
  ),
  -- volume contracts
 ordered_data AS (
    SELECT
        m.instrument_id,
        m.ts_recv,
        m.action, 
        m.size
    FROM futures_mbp m
    INNER JOIN futures i ON m.instrument_id = i.instrument_id
  ),
  aggregated_data AS (
      SELECT
          instrument_id,
          CAST(floor(ts_recv / 86400000000000) * 86400000000000 AS BIGINT) AS ts_event, 
          SUM(size) FILTER(WHERE action = 84) AS daily_volume
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
          i.expiration_date,
          LAG(daily_volume) OVER (
            PARTITION BY LEFT(ticker, 2), a.instrument_id 
            ORDER BY ts_event
          ) AS prev_daily_volume,
           LAG(daily_volume, 2) OVER (
            PARTITION BY LEFT(ticker, 2), a.instrument_id 
            ORDER BY ts_event
          ) AS prev2_daily_volume
      FROM aggregated_data a
      INNER JOIN futures i ON a.instrument_id = i.instrument_id
  ),
  ranked_volumes AS (
      SELECT 
          ticker,
          ts_event,
          instrument_id,
          ROW_NUMBER() OVER (
            PARTITION BY LEFT(ticker, 2), ts_event 
            ORDER BY 
              prev_daily_volume DESC NULLS LAST, 
              prev2_daily_volume DESC NULLS LAST,
              expiration_date ASC
        ) - 1 AS rank
      FROM daily_volumes
      WHERE ts_event > (SELECT MIN(ts_event) FROM daily_volumes)
  ),
  rolled_ranked_volumes AS (
      SELECT 
          rv.ts_event AS ts_day,  -- Add 1 day in nanoseconds
          LEFT(ticker, 2) || '.v.' || rv.rank AS continuous_ticker, 
          rv.instrument_id
      FROM ranked_volumes rv
  ),
  aligned_data AS(
      SELECT 
          rrv.*,
          i.instrument_id AS continuous_instrument_id
      FROM rolled_ranked_volumes rrv
      INNER JOIN futures i ON rrv.continuous_ticker = i.ticker
  ),
  volume_rollovers AS (
    SELECT
      *,
      CASE
        WHEN LAG(instrument_id) OVER (PARTITION BY continuous_ticker ORDER BY ts_day) IS NULL THEN 0
        WHEN LAG(instrument_id) OVER (PARTITION BY continuous_ticker ORDER BY ts_day) IS DISTINCT FROM instrument_id THEN 1
        ELSE 0
      END AS rollover_flag   
  FROM aligned_data
  ), 
  full_view AS (
    SELECT * FROM calendar_rollovers
    UNION ALL
    SELECT * FROM volume_rollovers
  )
  SELECT *
  FROM full_view
  ORDER BY ts_day, continuous_ticker;



-- -- Materialized view 
-- CREATE MATERIALIZED VIEW futures_continuous AS
--   -- calendar contracts 
--   WITH ts_bounds AS (
--       SELECT 
--           MIN(ts_recv) AS min_ts_recv, 
--           MAX(ts_recv) AS max_ts_recv 
--       FROM futures_mbp
--   ),
--   ts_days AS (
--       SELECT DISTINCT
--           FLOOR(ts_recv / 86400000000000.0) * 86400000000000 AS ts_day
--       FROM futures_mbp
--   ),
--   raw_data AS (
--       SELECT
--           ticker,
--           instrument_id,
--           expiration_date
--       FROM futures
--       WHERE is_continuous = false
--   ),
--   crossed AS (
--       SELECT
--           td.ts_day,
--           rd.ticker,
--           rd.instrument_id,
--           rd.expiration_date,
--           rd.expiration_date - td.ts_day AS distance
--       FROM ts_days td
--       CROSS JOIN raw_data rd
--       WHERE rd.expiration_date >= td.ts_day -- 
--   ),
--   ranked AS (
--       SELECT
--           cd.ts_day,
--           cd.ticker, 
--           cd.instrument_id,       
--           ROW_NUMBER() OVER (
--               PARTITION BY ts_day, LEFT(ticker, 2)
--               ORDER BY distance, expiration_date
--           ) - 1 AS rank
--       FROM crossed cd
--   ), 
--   c_continuous_ranked AS (
--       SELECT 
--         r.ts_day, 
--         LEFT(ticker, 2) || '.c.' || r.rank AS continuous_ticker, 
--         r.instrument_id
--       FROM ranked r 
--   ), 
--   c_continuous_lookup AS (
--       SELECT
--           ticker AS continuous_ticker,
--           instrument_id AS continuous_instrument_id
--       FROM futures
--       WHERE is_continuous = true
--   ), 
--   c_continuous AS (
--     SELECT
--         cr.ts_day,
--         cr.continuous_ticker,
--         cr.instrument_id,
--         cl.continuous_instrument_id
--     FROM c_continuous_ranked cr
--     LEFT JOIN c_continuous_lookup cl
--       ON cr.continuous_ticker = cl.continuous_ticker
--   ), 
--   calendar_rollovers AS (
--     SELECT
--       *,
--       CASE
--         WHEN LAG(instrument_id) OVER (PARTITION BY continuous_ticker ORDER BY ts_day) IS NULL THEN 0
--         WHEN LAG(instrument_id) OVER (PARTITION BY continuous_ticker ORDER BY ts_day) IS DISTINCT FROM instrument_id THEN 1
--         ELSE 0
--       END AS rollover_flag   
--   FROM c_continuous
--   ),
--   -- volume contracts
--   ordered_data AS (
--     SELECT
--         m.instrument_id,
--         m.ts_recv,
--         m.size
--     FROM futures_mbp m
--     INNER JOIN futures i ON m.instrument_id = i.instrument_id
--     WHERE m.action = 84  -- Filter only trades
--   ),
--   aggregated_data AS (
--       SELECT
--           instrument_id,
--           floor(ts_recv / 86400000000000) * 86400000000000 AS ts_event, 
--           SUM(size) AS volume
--       FROM ordered_data
--       GROUP BY
--           instrument_id,
--           floor(ts_recv / 86400000000000) * 86400000000000
--   ),
--   daily_volumes AS (
--       SELECT 
--           i.ticker, 
--           a.instrument_id,
--           CAST(a.ts_event AS BIGINT) AS ts_event, -- Explicitly cast to BIGINT to avoid scientific notation
--           a.volume AS daily_volume, -- Keep daily volume for each day
--           i.expiration_date 
--       FROM aggregated_data a
--       INNER JOIN futures i ON a.instrument_id = i.instrument_id
--   ),
--   ranked_volumes AS (
--       SELECT 
--           ticker,
--           ts_event,
--           instrument_id,
--           daily_volume,
--           expiration_date,
--           RANK() OVER (PARTITION BY LEFT(ticker, 2), ts_event ORDER BY daily_volume DESC) - 1 AS rank 
--       FROM daily_volumes
--   ),
--   rolled_ranked_volumes AS (
--       SELECT 
--           rv.ts_event + (86400000000000) AS ts_day,  -- Add 1 day in nanoseconds
--           LEFT(ticker, 2) || '.v.' || rv.rank AS continuous_ticker, 
--           rv.instrument_id
--       FROM ranked_volumes rv
--   ),
--   v_ts_bounds AS (
--       SELECT 
--           MIN(ts_recv) AS min_ts_recv, 
--           MAX(ts_recv) AS max_ts_recv 
--       FROM futures_mbp
--   ),
--   v_ts_days AS (
--       SELECT DISTINCT
--           FLOOR(ts_recv / 86400000000000.0) * 86400000000000 AS ts_day
--       FROM futures_mbp
--   ),
--   aligned_ranked AS (
--     SELECT 
--       t.ts_day, 
--       rrv.continuous_ticker, 
--       rrv.instrument_id
--     FROM v_ts_days t
--     LEFT JOIN rolled_ranked_volumes rrv ON t.ts_day = rrv.ts_day
--   ),
--   v_continuous_ranked AS (
--       SELECT 
--           ts_day,
--           MAX(instrument_id) FILTER (WHERE instrument_id IS NOT NULL) OVER (
--               ORDER BY ts_day
--               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
--           ) AS instrument_id,
--           MAX(continuous_ticker) FILTER (WHERE continuous_ticker IS NOT NULL) OVER (
--               ORDER BY ts_day
--               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
--           ) AS continuous_ticker
--       FROM aligned_ranked
--   ), 
--   v_continuous_lookup AS (
--       SELECT
--           ticker AS continuous_ticker,
--           instrument_id AS continuous_instrument_id
--       FROM futures
--       WHERE is_continuous = true
--   ), 
--   v_continuous AS (
--     SELECT
--         cr.ts_day,
--         cr.continuous_ticker,
--         cr.instrument_id,
--         cl.continuous_instrument_id
--     FROM v_continuous_ranked cr
--     LEFT JOIN v_continuous_lookup cl
--         ON cr.continuous_ticker = cl.continuous_ticker
--     WHERE cr.instrument_id IS NOT NULL -- first row shoudl be only dropped
--   ), 
--   volume_rollovers AS (
--     SELECT
--       *,
--       CASE
--         WHEN LAG(instrument_id) OVER (PARTITION BY continuous_ticker ORDER BY ts_day) IS NULL THEN 0
--         WHEN LAG(instrument_id) OVER (PARTITION BY continuous_ticker ORDER BY ts_day) IS DISTINCT FROM instrument_id THEN 1
--         ELSE 0
--       END AS rollover_flag   
--   FROM v_continuous
--   ), 
--   full_view AS (
--     SELECT * FROM calendar_rollovers
--     UNION ALL
--     SELECT * FROM volume_rollovers
--   )
--   SELECT *
--   FROM full_view
--   ORDER BY ts_day, continuous_ticker;
--
