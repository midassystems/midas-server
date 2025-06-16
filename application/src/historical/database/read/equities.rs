pub const EQUITIES_MBP1_QUERY: &str = r#"
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
    FROM equities_mbp m
    INNER JOIN equities i ON m.instrument_id = i.instrument_id
    LEFT JOIN equities_bid_ask b ON m.id = b.mbp_id AND b.depth = 0
    WHERE m.ts_recv BETWEEN $1 AND $2
    AND i.ticker = ANY($3)
    AND ($4 IS FALSE OR m.action = 84)
    ORDER BY m.ts_recv
"#;

pub const EQUITIES_TRADE_QUERY: &str = r#"
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
        0 AS rollover_flag
    FROM equities_mbp m
    INNER JOIN equities i ON m.instrument_id = i.instrument_id
    LEFT JOIN equities_bid_ask b ON m.id = b.mbp_id AND b.depth = 0
    WHERE m.ts_recv BETWEEN $1 AND $2
    AND i.ticker = ANY($3)
    AND m.action = 84  
    ORDER BY m.ts_event
"#;

pub const EQUITIES_OHLCV_QUERY: &str = r#"
    WITH ordered_data AS (
      SELECT
        m.instrument_id,
        m.ts_recv,
        m.price,
        m.size,
        row_number() OVER (PARTITION BY m.instrument_id, floor(m.ts_recv / $3) * $3 ORDER BY m.ts_recv ASC,  m.ctid ASC) AS first_row,
        row_number() OVER (PARTITION BY m.instrument_id, floor(m.ts_recv / $3) * $3 ORDER BY m.ts_recv DESC, m.ctid DESC) AS last_row
      FROM equities_mbp m
      INNER JOIN equities i ON m.instrument_id = i.instrument_id
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
      a.volume,
      0 AS rollover_flag
    FROM aggregated_data a
    INNER JOIN equities i ON a.instrument_id = i.instrument_id
    ORDER BY a.ts_event
"#;

pub const EQUITIES_BBO_QUERY: &str = r#"
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
      FROM equities_mbp m
      INNER JOIN equities i ON m.instrument_id = i.instrument_id
      LEFT JOIN equities_bid_ask b ON m.id = b.mbp_id AND b.depth = 0
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
