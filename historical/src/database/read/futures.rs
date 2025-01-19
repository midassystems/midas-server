pub const FUTURES_MBP1_QUERY: &str = r#"
            SELECT m.instrument_id, m.ts_event, m.price, m.size, m.action, m.side, m.flags, m.ts_recv, m.ts_in_delta, m.sequence, m.discriminator, i.ticker,
                   b.bid_px, b.bid_sz, b.bid_ct, b.ask_px, b.ask_sz, b.ask_ct
            FROM futures_mbp m
            INNER JOIN futures i ON m.instrument_id = i.instrument_id
            LEFT JOIN futures_bid_ask b ON m.id = b.mbp_id AND b.depth = 0
            WHERE m.ts_recv BETWEEN $1 AND $2
            AND i.ticker = ANY($3)
            AND ($4 IS FALSE OR m.action = 84)
            "#;

pub const FUTURES_TRADE_QUERY: &str = r#"
            SELECT m.instrument_id, m.ts_event, m.price, m.size, m.action, m.side, m.flags, m.ts_recv, m.ts_in_delta, m.sequence, i.ticker
            FROM futures_mbp m
            INNER JOIN futures i ON m.instrument_id = i.instrument_id
            LEFT JOIN futures_bid_ask b ON m.id = b.mbp_id AND b.depth = 0
            WHERE m.ts_recv BETWEEN $1 AND $2
            AND i.ticker = ANY($3)
            AND m.action = 84  -- Filter only trades where action is 'T' (ASCII 84)
            ORDER BY m.ts_event
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
          a.volume,
          i.ticker
        FROM aggregated_data a
        INNER JOIN futures i ON a.instrument_id = i.instrument_id
        ORDER BY a.ts_event
        "#;

pub const FUTURES_BBO_QUERY: &str = r#"
            WITH ordered_data AS (
                SELECT
                    m.id,
                    m.instrument_id,
                    m.ts_event,
                    m.price,
                    m.size,
                    m.action,
                    m.side,
                    m.flags,
                    m.sequence,
                    m.ts_recv,
                    b.bid_px,
                    b.ask_px,
                    b.bid_sz,
                    b.ask_sz,
                    b.bid_ct,
                    b.ask_ct,
                    row_number() OVER (PARTITION BY m.instrument_id, floor((m.ts_recv - 1) / $3) * $3 ORDER BY m.ts_recv ASC, m.ctid ASC) AS first_row,
                    row_number() OVER (PARTITION BY m.instrument_id, floor((m.ts_recv - 1) / $3) * $3 ORDER BY m.ts_recv DESC, m.ctid DESC) AS last_row
                FROM futures_mbp m
                INNER JOIN futures i ON m.instrument_id = i.instrument_id
                LEFT JOIN futures_bid_ask b ON m.id = b.mbp_id AND b.depth = 0
                WHERE m.ts_recv BETWEEN ($1 - 86400000000000) AND $2
                AND i.ticker = ANY($4)
            ),
            -- Subquery to get the last trade event
            trade_data AS (
                SELECT
                    instrument_id,
                    floor((ts_recv - 1) / $3) * $3 AS ts_recv_start,
                    MAX(ts_recv) AS last_trade_ts_recv,
                    MAX(id) AS last_trade_id
                FROM ordered_data
                WHERE action = 84 -- Only consider trades (action = 84)
                GROUP BY instrument_id, floor((ts_recv - 1) / $3) * $3
            ),
            aggregated_data AS (
                SELECT
                    o.instrument_id,
                    floor((o.ts_recv - 1) / $3) * $3 AS ts_recv,
                    MAX(o.ts_event) FILTER (WHERE o.ts_recv = t.last_trade_ts_recv AND o.id = t.last_trade_id AND o.action = 84) AS ts_event,  -- Correct reference for ts_event
                    MIN(o.bid_px) FILTER (WHERE o.last_row = 1) AS last_bid_px,
                    MIN(o.ask_px) FILTER (WHERE o.last_row = 1) AS last_ask_px,
                    MIN(o.bid_sz) FILTER (WHERE o.last_row = 1) AS last_bid_sz,
                    MIN(o.ask_sz) FILTER (WHERE o.last_row = 1) AS last_ask_sz,
                    MIN(o.bid_ct) FILTER (WHERE o.last_row = 1) AS last_bid_ct,
                    MIN(o.ask_ct) FILTER (WHERE o.last_row = 1) AS last_ask_ct,
                    MAX(o.price) FILTER (WHERE o.ts_recv = t.last_trade_ts_recv AND o.id = t.last_trade_id AND o.action = 84) AS last_trade_price,
                    MAX(o.size) FILTER (WHERE o.ts_recv = t.last_trade_ts_recv AND o.id = t.last_trade_id AND o.action = 84) AS last_trade_size,
                    MAX(o.side) FILTER (WHERE o.ts_recv = t.last_trade_ts_recv AND o.id = t.last_trade_id AND o.action = 84) AS last_trade_side,
                    MAX(o.flags) FILTER (WHERE o.last_row = 1) AS last_trade_flags,
                    MIN(o.sequence) FILTER (WHERE o.last_row = 1) AS last_trade_sequence
                FROM ordered_data o
                LEFT JOIN trade_data t ON o.instrument_id = t.instrument_id AND floor((o.ts_recv - 1) / $3) * $3 = t.ts_recv_start
                GROUP BY o.instrument_id, floor((o.ts_recv - 1) / $3) * $3, t.last_trade_ts_recv, t.last_trade_id
            ),
            -- Step 1: Forward-fill ts_event
            filled_ts_event AS (
                SELECT
                    a.instrument_id,
                    MAX(a.ts_event) OVER (PARTITION BY a.instrument_id ORDER BY a.ts_recv ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ts_event,  -- Forward-fill ts_event
                    a.ts_recv,
                    a.last_bid_px,
                    a.last_ask_px,
                    a.last_bid_sz,
                    a.last_ask_sz,
                    a.last_bid_ct,
                    a.last_ask_ct,
                    a.last_trade_price,
                    a.last_trade_size,
                    a.last_trade_side,
                    a.last_trade_flags,
                    a.last_trade_sequence
                FROM aggregated_data a
            ),
            -- Step 2: Forward-fill price and size based on the now-filled ts_event
            filled_price_size AS (
                SELECT
                    f.instrument_id,
                    f.ts_event,
                    CAST(f.ts_recv + $3 AS BIGINT) AS ts_recv,
                    find_last_ignore_nulls(f.last_trade_price) OVER (PARTITION BY f.instrument_id ORDER BY f.ts_recv) AS price,
                    find_last_ignore_nulls(f.last_trade_size) OVER (PARTITION BY f.instrument_id ORDER BY f.ts_recv) AS size,
                    find_last_ignore_nulls(f.last_trade_side) OVER (PARTITION BY f.instrument_id ORDER BY f.ts_recv) AS side,
                    f.last_bid_px AS bid_px,
                    f.last_ask_px AS ask_px,
                    f.last_bid_sz AS bid_sz,
                    f.last_ask_sz AS ask_sz,
                    f.last_bid_ct AS bid_ct,
                    f.last_ask_ct AS ask_ct,
                    f.last_trade_flags AS flags,
                    f.last_trade_sequence AS sequence
                FROM filled_ts_event f
            )
            SELECT
                fp.instrument_id,
                fp.ts_event,
                fp.ts_recv,
                fp.bid_px,
                fp.ask_px,
                fp.bid_sz,
                fp.ask_sz,
                fp.bid_ct,
                fp.ask_ct,
                fp.price,  -- Forward-filled price based on ts_event
                fp.size,   -- Forward-filled size based on ts_event
                fp.side,
                fp.flags,
                fp.sequence,
                i.ticker
            FROM filled_price_size fp
            INNER JOIN futures i ON fp.instrument_id = i.instrument_id
            WHERE fp.ts_recv BETWEEN $1 AND ($2 - $3)
            ORDER BY fp.ts_recv;
            "#;
