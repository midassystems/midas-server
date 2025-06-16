pub const FUTURES_MBP1_QUERY: &str = r#"
    SELECT 
        m.instrument_id, 
        m.ts_event, 
        m.price, 
        m.size, 
        m.action, 
        m.side, 
        m.flags, 
        m.ts_recv, 
        m.ts_in_delta, 
        m.sequence, 
        m.discriminator,
        0 AS rollover_flag,
        b.bid_px, 
        b.bid_sz, 
        b.bid_ct, 
        b.ask_px, 
        b.ask_sz, 
        b.ask_ct
    FROM futures_mbp m
    INNER JOIN futures i ON m.instrument_id = i.instrument_id
    LEFT JOIN futures_bid_ask b ON m.id = b.mbp_id AND b.depth = 0
    WHERE m.ts_recv BETWEEN $1 AND $2
    AND i.ticker = ANY($3)
    AND ($4 IS FALSE OR m.action = 84)
    ORDER BY m.ts_recv
"#;

pub const FUTURES_TRADE_QUERY: &str = r#"
    SELECT 
        m.instrument_id, 
        m.ts_event, 
        m.price, 
        m.size, 
        m.action, 
        m.side, 
        m.flags, 
        m.ts_recv, 
        m.ts_in_delta, 
        0 AS rollover_flag,
        m.sequence
    FROM futures_mbp m
    INNER JOIN futures i ON m.instrument_id = i.instrument_id
    LEFT JOIN futures_bid_ask b ON m.id = b.mbp_id AND b.depth = 0
    WHERE m.ts_recv BETWEEN $1 AND $2
    AND i.ticker = ANY($3)
    AND m.action = 84  
    ORDER BY m.ts_recv
"#;

pub const FUTURES_OHLCV_QUERY: &str = r#"
    WITH ordered_data AS (
      SELECT
        m.instrument_id,
        m.ts_recv,
        m.price,
        m.size,
        row_number() OVER (PARTITION BY m.instrument_id, floor(m.ts_recv / $3) * $3 ORDER BY m.ts_recv ASC,  m.ctid ASC) AS first_row,
        row_number() OVER (PARTITION BY m.instrument_id, floor(m.ts_recv / $3) * $3 ORDER BY m.ts_recv DESC, m.ctid DESC) AS last_row
      FROM futures_mbp m
      INNER JOIN futures i ON m.instrument_id = i.instrument_id
      WHERE m.ts_recv BETWEEN $1 AND $2
      AND i.ticker = ANY($4)
      AND m.action = 84  -- Filter only trades where action is 'T' (ASCII 84)
    ),
    aggregated_data AS (
      SELECT
        instrument_id,
        floor(ts_recv / $3) * $3 AS ts_event, -- Maintain nanoseconds
        MIN(price) FILTER (WHERE first_row = 1) AS open,
        MIN(price) FILTER (WHERE last_row = 1) AS close,
        MIN(price) AS low,
        MAX(price) AS high,
        SUM(size) AS volume
      FROM ordered_data
      GROUP BY
        instrument_id,
        floor(ts_recv / $3) * $3
    )
    SELECT
      a.instrument_id,
      CAST(a.ts_event AS BIGINT), -- Keep as nanoseconds
      a.open,
      a.close,
      a.low,
      a.high,
      0 AS rollover_flag,
      a.volume
    FROM aggregated_data a
    INNER JOIN futures i ON a.instrument_id = i.instrument_id
    ORDER BY a.ts_event
"#;

pub const FUTURES_BBO_QUERY: &str = r#"
    WITH ordered_data AS (
      SELECT
          m.id,
          m.instrument_id,
          m.ts_recv,
          b.bid_px,
          b.ask_px,
          b.bid_sz,
          b.ask_sz,
          b.bid_ct,
          b.ask_ct,
          0 AS rollover_flag,
          row_number() OVER (
            PARTITION BY m.instrument_id, floor((m.ts_recv - 1) / $3) * $3
            ORDER BY m.ts_recv DESC, m.ctid DESC
          ) AS last_row
      FROM futures_mbp m
      INNER JOIN futures i ON m.instrument_id = i.instrument_id
      LEFT JOIN futures_bid_ask b ON m.id = b.mbp_id AND b.depth = 0
      WHERE m.ts_recv BETWEEN $1 AND $2
      AND i.ticker = ANY($4)
    ),
    last_rows AS (
      SELECT *
      FROM ordered_data
      WHERE last_row = 1
    ),
    aggregated_data AS (
      SELECT
        instrument_id,
        CAST(CEIL(ts_recv::double precision / $3) * $3 AS BIGINT) AS ts_event,
        rollover_flag,
        bid_px,
        ask_px,
        bid_sz,
        ask_sz,
        bid_ct,
        ask_ct
      FROM last_rows
    )
    SELECT
      b.instrument_id,
      b.ts_event,
      b.rollover_flag,
      b.bid_px,
      b.ask_px,
      b.bid_sz,
      b.ask_sz,
      b.bid_ct,
      b.ask_ct
    FROM aggregated_data b
    ORDER BY b.ts_event
"#;

// Rollover flag is assigned to next ts_recv after start of rollover day
// Rolover could be a day lagged if there was no activity on the day of rollover (TBBO)
pub const FUTURES_CONTINUOUS_MBP1_QUERY: &str = r#"
    WITH continuous_tickers AS (
        SELECT *
        FROM futures_continuous
        WHERE ts_day BETWEEN $1 AND $2
        AND continuous_ticker = ANY($3)
    ), 
    filtered_mbp AS (
      SELECT
        ct.continuous_instrument_id,
        mp.*,
        ROW_NUMBER() OVER (
          PARTITION BY ct.continuous_ticker, ct.ts_day
          ORDER BY mp.ts_recv
        ) AS rn
      FROM continuous_tickers ct
      INNER JOIN futures_mbp mp
        ON ct.instrument_id = mp.instrument_id
        AND mp.ts_recv >= ct.ts_day
        AND mp.ts_recv < ct.ts_day + 86400000000000
        AND ($4 IS FALSE OR mp.action = 84)  
    ),
    rollovers AS (
      SELECT
        continuous_instrument_id,
        ts_day
      FROM futures_continuous
      WHERE rollover_flag = 1
      AND continuous_ticker = ANY($3)
    ),
    rollover_event AS (
      SELECT DISTINCT ON (r.continuous_instrument_id, r.ts_day)
        mp.*,
        1 AS rollover_flag
      FROM rollovers r
      JOIN filtered_mbp mp
      ON mp.continuous_instrument_id = r.continuous_instrument_id
      AND mp.ts_recv >= r.ts_day
      ORDER BY r.continuous_instrument_id, r.ts_day, mp.ts_recv
    ),
    final_data AS (
      SELECT 
         f.*,
        COALESCE(r.rollover_flag, 0) AS rollover_flag
      FROM filtered_mbp f
      LEFT JOIN rollover_event r
        ON f.continuous_instrument_id = r.continuous_instrument_id
       AND f.ts_recv = r.ts_recv
    )
    SELECT 
      m.continuous_instrument_id AS instrument_id, 
      m.ts_event, 
      m.price, 
      m.size, 
      m.action, 
      m.side, 
      m.flags, 
      m.ts_recv, 
      m.ts_in_delta, 
      m.sequence, 
      m.discriminator,
      m.rollover_flag,
      b.bid_px, 
      b.bid_sz, 
      b.bid_ct, 
      b.ask_px, 
      b.ask_sz, 
      b.ask_ct
    FROM final_data m
    LEFT JOIN futures_bid_ask b ON m.id = b.mbp_id AND b.depth = 0
    WHERE m.ts_recv BETWEEN $1 AND $2
    ORDER BY m.ts_recv
"#;

pub const FUTURES_CONTINUOUS_TRADE_QUERY: &str = r#"
    WITH continuous_tickers AS (
        SELECT *
        FROM futures_continuous
        WHERE ts_day BETWEEN $1 AND $2
        AND continuous_ticker = ANY($3)
    ),
    filtered_mbp AS (
      SELECT
        ct.continuous_instrument_id,
        mp.*,
        ROW_NUMBER() OVER (
          PARTITION BY ct.continuous_ticker, ct.ts_day
          ORDER BY mp.ts_recv
        ) AS rn
      FROM continuous_tickers ct
      INNER JOIN futures_mbp mp
        ON ct.instrument_id = mp.instrument_id
        AND mp.ts_recv >= ct.ts_day
        AND mp.ts_recv < ct.ts_day + 86400000000000
        AND mp.action = 84
    ),
    rollovers AS (
      SELECT
        continuous_instrument_id,
        ts_day
      FROM futures_continuous
      WHERE rollover_flag = 1
      AND continuous_ticker = ANY($3)
    ),
    rollover_event AS (
      SELECT DISTINCT ON (r.continuous_instrument_id, r.ts_day)
        mp.*,
        1 AS rollover_flag
      FROM rollovers r
      JOIN filtered_mbp mp
      ON mp.continuous_instrument_id = r.continuous_instrument_id
      AND mp.ts_recv >= r.ts_day
      ORDER BY r.continuous_instrument_id, r.ts_day, mp.ts_recv
    ),
    final_data AS (
      SELECT 
         f.*,
        COALESCE(r.rollover_flag, 0) AS rollover_flag
      FROM filtered_mbp f
      LEFT JOIN rollover_event r
        ON f.continuous_instrument_id = r.continuous_instrument_id
       AND f.ts_recv = r.ts_recv
    )
    SELECT 
      m.continuous_instrument_id AS instrument_id, 
      m.ts_event, 
      m.price, 
      m.size, 
      m.action, 
      m.side, 
      m.flags, 
      m.ts_recv, 
      m.ts_in_delta, 
      m.sequence, 
      m.rollover_flag
    FROM final_data m
    WHERE m.ts_recv BETWEEN $1 AND $2
    ORDER BY m.ts_recv
"#;

pub const FUTURES_CONTINUOUS_OHLCV_QUERY: &str = r#"
    WITH continuous_tickers AS (
        SELECT *
        FROM futures_continuous
        WHERE ts_day BETWEEN $1 AND $2
        AND continuous_ticker = ANY($4)
    ),
    filtered_mbp AS (
      SELECT
        ct.continuous_instrument_id,
        mp.ts_recv, 
        mp.price, 
        mp.size,
        mp.instrument_id,
        ROW_NUMBER() OVER (
          PARTITION BY ct.continuous_instrument_id, floor(mp.ts_recv / $3) * $3
          ORDER BY mp.ts_recv ASC,  mp.ctid ASC
        ) AS first_row,
        ROW_NUMBER() OVER (
          PARTITION BY ct.continuous_instrument_id, floor(mp.ts_recv / $3) * $3
          ORDER BY mp.ts_recv DESC, mp.ctid DESC
        ) AS last_row
      FROM continuous_tickers ct
      INNER JOIN futures_mbp mp
        ON ct.instrument_id = mp.instrument_id
        AND mp.ts_recv >= ct.ts_day
        AND mp.ts_recv < ct.ts_day + 86400000000000
        AND mp.action = 84
    ),
    rollovers AS (
      SELECT
        continuous_instrument_id,
        ts_day
      FROM futures_continuous
      WHERE rollover_flag = 1
      AND continuous_ticker = ANY($4)
    ),
    rollover_event AS (
      SELECT DISTINCT ON (r.continuous_instrument_id, r.ts_day)
        mp.*,
        1 AS rollover_flag
      FROM rollovers r
      JOIN filtered_mbp mp
      ON mp.continuous_instrument_id = r.continuous_instrument_id
      AND mp.ts_recv >= r.ts_day
      ORDER BY r.continuous_instrument_id, r.ts_day, mp.ts_recv
    ),
    rollover_data AS (
      SELECT 
         f.*,
        COALESCE(r.rollover_flag, 0) AS rollover_flag
      FROM filtered_mbp f
      LEFT JOIN rollover_event r
        ON f.continuous_instrument_id = r.continuous_instrument_id
       AND f.ts_recv = r.ts_recv
    ),
    final_data AS (
      SELECT 
        rd.continuous_instrument_id AS instrument_id,
        CAST(floor(rd.ts_recv / $3) * $3 AS BIGINT) AS ts_event, -- Maintain nanoseconds
        MAX(rd.rollover_flag) as rollover_flag,
        MIN(rd.price) FILTER (WHERE rd.first_row = 1) AS open,
        MIN(rd.price) FILTER (WHERE rd.last_row = 1) AS close,
        MIN(rd.price) AS low,
        MAX(rd.price) AS high,
        SUM(rd.size) AS volume
      FROM rollover_data rd
    GROUP BY
      rd.continuous_instrument_id,
      floor(ts_recv / $3) * $3
    )
    SELECT * 
    FROM final_data 
    WHERE ts_event BETWEEN $1 AND ($2 - 1)
    ORDER BY ts_event 
"#;

pub const FUTURES_CONTINUOUS_BBO_QUERY: &str = r#"
    WITH continuous_tickers AS (
        SELECT *
        FROM futures_continuous
        WHERE ts_day BETWEEN $1 AND $2
        AND continuous_ticker = ANY($4)
    ),
    filtered_mbp AS (
      SELECT
        ct.continuous_instrument_id,   
        mp.id,
        mp.ts_recv,
        mp.instrument_id,
        FLOOR(mp.ts_recv / $3) * $3 AS ts_bucket,
        ROW_NUMBER() OVER (
          PARTITION BY mp.instrument_id, floor(mp.ts_recv / $3) * $3
          ORDER BY mp.ts_recv DESC, mp.ctid DESC
        ) AS last_row
      FROM continuous_tickers ct
      INNER JOIN futures_mbp mp
        ON ct.instrument_id = mp.instrument_id
        AND mp.ts_recv >= ct.ts_day
        AND mp.ts_recv < ct.ts_day + 86400000000000
    ),
    rollovers AS (
      SELECT
        continuous_instrument_id,
        ts_day
      FROM futures_continuous
      WHERE rollover_flag = 1
      AND continuous_ticker = ANY($4)
    ),
    rollover_event AS (
      SELECT DISTINCT ON (r.continuous_instrument_id, r.ts_day)
        mp.*,
        1 AS rollover_flag
      FROM rollovers r
      JOIN filtered_mbp mp
      ON mp.continuous_instrument_id = r.continuous_instrument_id
      AND mp.ts_recv >= r.ts_day
      ORDER BY r.continuous_instrument_id, r.ts_day, mp.ts_recv
    ),
    rollover_data AS (
      SELECT 
         f.*,
        COALESCE(r.rollover_flag, 0) AS rollover_flag
      FROM filtered_mbp f
      LEFT JOIN rollover_event r
        ON f.continuous_instrument_id = r.continuous_instrument_id
       AND f.ts_recv = r.ts_recv
    ),
    with_max_flag AS (
        SELECT *,
               MAX(rollover_flag) OVER (
                   PARTITION BY continuous_instrument_id, ts_bucket
               ) AS max_rollover_flag
        FROM rollover_data
    ),
    final_data AS (
      SELECT
        w.id,
        w.continuous_instrument_id AS instrument_id,
        CAST(CEIL(w.ts_recv::double precision / $3) * $3 AS BIGINT) AS ts_event,
        max_rollover_flag AS rollover_flag
      FROM with_max_flag w
      WHERE last_row = 1
    )
    SELECT 
      f.instrument_id, 
      f.ts_event,
      f.rollover_flag,
      b.bid_px, 
      b.bid_sz, 
      b.bid_ct, 
      b.ask_px, 
      b.ask_sz, 
      b.ask_ct
    FROM final_data f
    LEFT JOIN futures_bid_ask b ON f.id = b.mbp_id AND b.depth = 0
    WHERE ts_event BETWEEN $1 AND ($2 - 1)
    ORDER BY ts_event
"#;
