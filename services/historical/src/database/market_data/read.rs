use crate::Result;
use async_trait::async_trait;
use futures::Stream;
use mbn::encode::RecordEncoder;
use mbn::enums::{RType, Schema};
use mbn::records::{BboMsg, BidAskPair, Mbp1Msg, OhlcvMsg, RecordHeader, TbboMsg, TradeMsg};
use mbn::{record_enum::RecordEnum, symbols::SymbolMap};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use std::os::raw::c_char;
use std::pin::Pin;
use std::str::FromStr;
use tracing::{info, error};
use futures::stream::StreamExt;


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RetrieveParams {
    pub symbols: Vec<String>,
    pub start_ts: i64,
    pub end_ts: i64,
    pub schema: String,
}

impl RetrieveParams {
    fn schema(&self) -> Result<Schema> {
        let schema = Schema::from_str(&self.schema)?;
        Ok(schema)
    }

    fn rtype(&self) -> Result<RType> {
        let schema = Schema::from_str(&self.schema)?;
        Ok(RType::from(schema))
    }

    fn schema_interval(&self) -> Result<i64> {
        let schema = Schema::from_str(&self.schema)?;

        match schema {
            Schema::Mbp1 => Ok(1), // 1 nanosecond
            Schema::Trade => Ok(1),
            Schema::Tbbo => Ok(1),
            Schema::Ohlcv1S => Ok(1_000_000_000), // 1 second in nanoseconds
            Schema::Ohlcv1M => Ok(60_000_000_000), // 1 minute in nanoseconds
            Schema::Ohlcv1H => Ok(3_600_000_000_000), // 1 hour in nanoseconds
            Schema::Ohlcv1D => Ok(86_400_000_000_000), // 1 day in nanoseconds
            Schema::Bbo1S => Ok(1_000_000_000),
            Schema::Bbo1M => Ok(60_000_000_000),
        }
    }

    fn interval_adjust_ts_start(&mut self) -> Result<()> {
        let interval_ns =self.schema_interval()?;

        if self.start_ts % interval_ns == 0 {
            // If the timestamp is already aligned to the interval, return it as is
            Ok(())
        } else {
            self.start_ts = self.start_ts - (self.start_ts % interval_ns);
            Ok(())
        }
    }

    fn interval_adjust_ts_end(&mut self) -> Result<()> {
        let interval_ns =self.schema_interval()?;

        if self.end_ts % interval_ns == 0 {
            // If the timestamp is already aligned to the interval, return it as is
            Ok(())
        } else {
            // If not aligned, round up to the next interval boundary
            self.end_ts = self.end_ts + (interval_ns - (self.end_ts % interval_ns));
            Ok(())
        }
    }

}


#[async_trait]
pub trait RecordsQuery{
    async fn retrieve_query(
        pool: &PgPool,
        params: &mut RetrieveParams,
    ) -> Result<
        Pin<Box<dyn Stream<Item = std::result::Result<sqlx::postgres::PgRow, sqlx::Error>> + Send>>,
    >;

}

pub trait FromRow: Sized {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self>;
}


#[async_trait]
impl RecordsQuery for Mbp1Msg {
    async fn retrieve_query(
        pool: &PgPool,
        params: &mut RetrieveParams,
    ) -> Result<
        Pin<Box<dyn Stream<Item = std::result::Result<sqlx::postgres::PgRow, sqlx::Error>> + Send>>,
    > {
        // Parameters
        let _ = params.interval_adjust_ts_start()?;
        let _ = params.interval_adjust_ts_end()?; 
        let tbbo_flag = params.schema()? == Schema::Tbbo;
        let symbol_array: Vec<String> = params.symbols.iter().map(|s| s.clone()).collect(); // Ownership fix

        info!(
            "Retrieving {:?} records for symbols: {:?} start: {:?} end: {:?} tbbo_flag {:?}",
            params.schema, params.symbols, params.start_ts, params.end_ts, tbbo_flag
        );

        // Query to Cursor
        let cursor = sqlx::query( 
            r#"
            SELECT m.instrument_id, m.ts_event, m.price, m.size, m.action, m.side, m.flags, m.ts_recv, m.ts_in_delta, m.sequence, i.ticker,
                   b.bid_px, b.bid_sz, b.bid_ct, b.ask_px, b.ask_sz, b.ask_ct
            FROM mbp m
            INNER JOIN instrument i ON m.instrument_id = i.id
            LEFT JOIN bid_ask b ON m.id = b.mbp_id AND b.depth = 0
            WHERE m.ts_recv BETWEEN $1 AND $2
            AND i.ticker = ANY($3)
            AND ($4 IS FALSE OR m.action = 84)
            "#)
            .bind(params.start_ts)
            .bind(params.end_ts - 1)
            .bind(symbol_array)
            .bind(tbbo_flag)
            .fetch(pool);

        Ok(cursor)
    }


}


impl FromRow for Mbp1Msg {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self> {
        Ok(Mbp1Msg {
            hd: RecordHeader::new::<Mbp1Msg>(
                row.try_get::<i32, _>("instrument_id")? as u32,
                row.try_get::<i64, _>("ts_event")? as u64,
            ),
            price: row.try_get::<i64, _>("price")?,
            size: row.try_get::<i32, _>("size")? as u32,
            action: row.try_get::<i32, _>("action")? as c_char,
            side: row.try_get::<i32, _>("side")? as c_char,
            flags: row.try_get::<i32, _>("flags")? as u8,
            depth: 0 as u8, // Always top of book

            // flags: row.try_get::<i32, _>("flags")? as u8,
            ts_recv: row.try_get::<i64, _>("ts_recv")? as u64,
            ts_in_delta: row.try_get::<i32, _>("ts_in_delta")?,
            sequence: row.try_get::<i32, _>("sequence")? as u32,
            levels: [BidAskPair {
                bid_px: row.try_get::<i64, _>("bid_px")?,
                ask_px: row.try_get::<i64, _>("ask_px")?,
                bid_sz: row.try_get::<i32, _>("bid_sz")? as u32,
                ask_sz: row.try_get::<i32, _>("ask_sz")? as u32,
                bid_ct: row.try_get::<i32, _>("bid_ct")? as u32,
                ask_ct: row.try_get::<i32, _>("ask_ct")? as u32,
            }],
        })
    }
}

#[async_trait]
impl RecordsQuery for TradeMsg {
    async fn retrieve_query(
        pool: &PgPool,
        params: &mut RetrieveParams,
    ) -> Result<
        Pin<Box<dyn Stream<Item = std::result::Result<sqlx::postgres::PgRow, sqlx::Error>> + Send>>,
    > {
       // Parameters
        let _ = params.interval_adjust_ts_start()?;
        let _ = params.interval_adjust_ts_end()?; 
        let symbol_array: Vec<String> = params.symbols.iter().map(|s| s.clone()).collect(); // Ownership fix

        info!(
            "Retrieving {:?} records for symbols: {:?} start: {:?} end: {:?} ",
            params.schema, params.symbols, params.start_ts, params.end_ts
        );

        // Execute the query with parameters, including LIMIT and OFFSET
        let cursor = sqlx::query(
            r#"
            SELECT m.instrument_id, m.ts_event, m.price, m.size, m.action, m.side, m.flags, m.ts_recv, m.ts_in_delta, m.sequence, i.ticker
            FROM mbp m
            INNER JOIN instrument i ON m.instrument_id = i.id
            LEFT JOIN bid_ask b ON m.id = b.mbp_id AND b.depth = 0
            WHERE m.ts_recv BETWEEN $1 AND $2
            AND i.ticker = ANY($3)
            AND m.action = 84  -- Filter only trades where action is 'T' (ASCII 84)
            ORDER BY m.ts_event
            "#)
            .bind(params.start_ts)
            .bind(params.end_ts - 1)
            .bind(symbol_array)
            .fetch(pool);

        Ok(cursor)
    }

}


impl FromRow for TradeMsg {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self> {
        Ok(TradeMsg {
            hd: RecordHeader::new::<TradeMsg>(
                row.try_get::<i32, _>("instrument_id")? as u32,
                row.try_get::<i64, _>("ts_event")? as u64,
            ),
            price: row.try_get::<i64, _>("price")?,
            size: row.try_get::<i32, _>("size")? as u32,
            action: row.try_get::<i32, _>("action")? as c_char,
            side: row.try_get::<i32, _>("side")? as c_char,
            flags: row.try_get::<i32, _>("flags")? as u8,
            depth: 0 as u8, // Always top of book
            ts_recv: row.try_get::<i64, _>("ts_recv")? as u64,
            ts_in_delta: row.try_get::<i32, _>("ts_in_delta")?,
            sequence: row.try_get::<i32, _>("sequence")? as u32,
        })
    }
}

#[async_trait]
impl RecordsQuery for BboMsg {
    async fn retrieve_query(
        pool: &PgPool,
        params: &mut RetrieveParams,
    ) -> Result<
        Pin<Box<dyn Stream<Item = std::result::Result<sqlx::postgres::PgRow, sqlx::Error>> + Send>>,
    > {
        // Parameters
        let _ = params.interval_adjust_ts_start()?;
        let _ = params.interval_adjust_ts_end()?; 
        let interval_ns = params.schema_interval()?;
        let symbol_array: Vec<String> = params.symbols.iter().map(|s| s.clone()).collect(); // Ownership fix

        info!(
            "Retrieving {:?} records for symbols: {:?} start: {:?} end: {:?} ",
            params.schema, params.symbols, params.start_ts, params.end_ts
        );

        // Construct the SQL query with a join and additional filtering by symbols
        let cursor = sqlx::query(
            r#"
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
                FROM mbp m
                INNER JOIN instrument i ON m.instrument_id = i.id
                LEFT JOIN bid_ask b ON m.id = b.mbp_id AND b.depth = 0
                WHERE m.ts_recv BETWEEN ($1 - 86400000000000) AND $2
                AND i.ticker = ANY($4)
            ),
            -- Subquery to get the last trade event
            trade_data AS (
                SELECT
                    instrument_id,
                    floor((ts_recv - 1) / $3) * $3 AS ts_recv_start,
                    MAX(ts_recv) AS last_trade_ts_recv,
                     --MAX(ts_event) AS last_trade_ts_event,
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
            INNER JOIN instrument i ON fp.instrument_id = i.id
            WHERE fp.ts_recv BETWEEN $1 AND ($2 - $3)
            ORDER BY fp.ts_recv;
            "#)
            .bind(params.start_ts)
            .bind(params.end_ts)
            .bind(interval_ns)
            .bind(symbol_array)
            .fetch(pool);

        Ok(cursor)
    }
    

}

impl FromRow for BboMsg {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self> {
        Ok(BboMsg {
            hd: RecordHeader::new::<BboMsg>(
                row.try_get::<i32, _>("instrument_id")? as u32,
                row.try_get::<i64, _>("ts_event").unwrap_or(0) as u64,
            ),
            price: row.try_get::<i64, _>("price").unwrap_or(0),
            size: row.try_get::<i32, _>("size").unwrap_or(0) as u32,
            side: row.try_get::<i32, _>("side").unwrap_or(78) as c_char,
            flags: row.try_get::<i32, _>("flags")? as u8,
            ts_recv: row.try_get::<i64, _>("ts_recv")? as u64,
            sequence: row.try_get::<i32, _>("sequence")? as u32,
            levels: [BidAskPair {
                bid_px: row.try_get::<i64, _>("bid_px")?,
                ask_px: row.try_get::<i64, _>("ask_px")?,
                bid_sz: row.try_get::<i32, _>("bid_sz")? as u32,
                ask_sz: row.try_get::<i32, _>("ask_sz")? as u32,
                bid_ct: row.try_get::<i32, _>("bid_ct")? as u32,
                ask_ct: row.try_get::<i32, _>("ask_ct")? as u32,
            }],
        })
    }
}

#[async_trait]
impl RecordsQuery for OhlcvMsg {
    async fn retrieve_query(
        pool: &PgPool,
        params: &mut RetrieveParams,
    ) -> Result<
        Pin<Box<dyn Stream<Item = std::result::Result<sqlx::postgres::PgRow, sqlx::Error>> + Send>>,
    > {
        // Parameters
        let _ = params.interval_adjust_ts_start()?;
        let _ = params.interval_adjust_ts_end()?; 
        let interval_ns = params.schema_interval()?;
        let symbol_array: Vec<String> = params.symbols.iter().map(|s| s.clone()).collect(); // Ownership fix

        info!(
            "Retrieving {:?} records for symbols: {:?} start: {:?} end: {:?} ",
            params.schema, params.symbols, params.start_ts, params.end_ts
        );

        let cursor = sqlx::query(
        r#"
        WITH ordered_data AS (
          SELECT
            m.instrument_id,
            m.ts_recv,
            m.price,
            m.size,
            row_number() OVER (PARTITION BY m.instrument_id, floor(m.ts_recv / $3) * $3 ORDER BY m.ts_recv ASC,  m.ctid ASC) AS first_row,
            row_number() OVER (PARTITION BY m.instrument_id, floor(m.ts_recv / $3) * $3 ORDER BY m.ts_recv DESC, m.ctid DESC) AS last_row
          FROM mbp m
          INNER JOIN instrument i ON m.instrument_id = i.id
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
        INNER JOIN instrument i ON a.instrument_id = i.id
        ORDER BY a.ts_event
        "#
        )
        .bind(params.start_ts)
        .bind(params.end_ts)
        .bind(interval_ns)
        .bind(symbol_array)
        .fetch(pool);

        Ok(cursor)
    }

}

impl FromRow for OhlcvMsg {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self> {
        Ok(OhlcvMsg {
            hd: RecordHeader::new::<OhlcvMsg>(
                row.try_get::<i32, _>("instrument_id")? as u32,
                row.try_get::<i64, _>("ts_event")? as u64,
            ),
            open: row.try_get::<i64, _>("open")?,
            close: row.try_get::<i64, _>("close")?,
            low: row.try_get::<i64, _>("low")?,
            high: row.try_get::<i64, _>("high")?,
            volume: row.try_get::<i64, _>("volume")? as u64,
        })
    }
}

#[async_trait]
impl RecordsQuery for RecordEnum {
    async fn retrieve_query(
        pool: &PgPool,
        params: &mut RetrieveParams,
    ) -> Result<Pin<Box<dyn Stream<Item = std::result::Result<sqlx::postgres::PgRow, sqlx::Error>> + Send>>> {

        match RType::from(params.rtype().unwrap()) {
            RType::Mbp1 => Ok(Mbp1Msg::retrieve_query(pool, params).await?),
            RType::Trade => Ok(TradeMsg::retrieve_query(pool, params).await?),
            RType::Ohlcv => Ok(OhlcvMsg::retrieve_query(pool, params).await?),
            RType::Bbo => Ok(BboMsg::retrieve_query(pool, params).await?),
            RType::Tbbo => Ok(TbboMsg::retrieve_query(pool, params).await?),
        }
    }
}


type FromRowFn = fn(&sqlx::postgres::PgRow) -> Result<RecordEnum>;

fn get_from_row_fn(rtype: RType) -> FromRowFn {
    match rtype {
        RType::Mbp1 => |row| Ok(RecordEnum::Mbp1(Mbp1Msg::from_row(row)?)),
        RType::Trade => |row| Ok(RecordEnum::Trade(TradeMsg::from_row(row)?)),
        RType::Ohlcv => |row| Ok(RecordEnum::Ohlcv(OhlcvMsg::from_row(row)?)),
        RType::Bbo => |row| Ok(RecordEnum::Bbo(BboMsg::from_row(row)?)),
        RType::Tbbo => |row| Ok(RecordEnum::Tbbo(TbboMsg::from_row(row)?)),
    }
}


// Helper function to process records
pub async fn process_records(
    pool: &PgPool,
    params: &mut RetrieveParams,
    batch_counter: &mut i64,
    record_cursor: &mut std::io::Cursor<Vec<u8>>,
    symbol_map: &mut SymbolMap,
) -> Result<()> {
    let rtype = RType::from(params.rtype().unwrap());
    let from_row_fn = get_from_row_fn(rtype);

    let mut encoder = RecordEncoder::new(record_cursor);
    let mut cursor = RecordEnum::retrieve_query(&pool, params).await?;

    info!("Processing queried records.");
    while let Some(row_result) = cursor.next().await {
        match row_result {
            Ok(row) => {
                let instrument_id = row.try_get::<i32, _>("instrument_id")? as u32;
                let ticker: String = row.try_get("ticker")?;
                
                // Use the from_row_fn here
                let record = from_row_fn(&row)?;

                // Convert to RecordEnum and add to encoder
                let record_ref = record.to_record_ref();
                encoder.encode_record(&record_ref).unwrap();

                // Update symbol map
                symbol_map.add_instrument(&ticker, instrument_id);

                // Increment the batch counter
                *batch_counter += 1;
            }
            Err(e) => {
                error!("Error processing row: {:?}", e);
                return Err(e.into());
            }
        }
    }

    Ok(())
}


#[cfg(test)]
mod test {
    use super::*;
    use crate::database::init::init_db;
    use crate::database::symbols::*;
    use mbn::enums::{Action, Side};
    use mbn::symbols::Instrument;
    use serial_test::serial;
    use mbn::symbols::Vendors;
    use crate::database::market_data::create::RecordInsertQueries;
    use sqlx::{PgPool, Postgres, Transaction};
    // use crate::database::market_data::create::validate_staging;

    async fn create_instrument(pool: &PgPool) -> Result<i32> {
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(            
            None,
            ticker,
            name,
            Vendors::Databento,
            Some("continuous".to_string()),
            Some("GLBX.MDP3".to_string()),
            1704672000000000000,
            1704672000000000000,
            true
        );
        let id = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        let _ = transaction.commit().await?;
        Ok(id)
    }
    
    async fn insert_records(
        tx: &mut Transaction<'_, Postgres>,
        records: Vec<Mbp1Msg>,
    ) -> Result<()> {
        for record in records {
            record.insert_query(tx).await?;
        }
        
        // let _ = validate_staging(tx).await?;
        // tx.commit().await?;

        Ok(())
    }


    #[test]
    fn test_retrieve_params_schema() -> anyhow::Result<()> {
        let params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: String::from("mbp-1"),
        };

        // Test
        assert_eq!(params.schema()?, Schema::Mbp1);

        Ok(())
    }
    #[test]
    fn test_retrieve_params_schema_interval() -> anyhow::Result<()> {
        let params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: String::from("tbbo"),
        };

        // Test
        assert_eq!(params.schema_interval()?, 1);

        Ok(())
    }

    #[test]
    fn test_retrieve_params_rtype() -> anyhow::Result<()> {
        let params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1728878401000000000,
            end_ts: 1728878460000000000,
            schema: String::from("ohlcv-1h"),
        };

        // Test
        assert_eq!(params.rtype()?, RType::Ohlcv);

        Ok(())
    }
  
   
    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_mbp1() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let instrument_id = create_instrument(&pool)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564) },
                price: 6770,
                size: 1,
                action: Action::Add as c_char,
                side: Side::Bid as c_char,
                depth: 0,
                flags: 0,
                ts_recv: 1704209103644092564,
                ts_in_delta: 17493,
                sequence: 739763,
                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092565) },
                price: 6870,
                size: 2,
                action: Action::Add as c_char,
                side: Side::Bid as c_char,
                depth: 0,
                flags: 0,
                ts_recv: 1704209103644092565,
                ts_in_delta: 17493,
                sequence: 739763,
                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
        ];

        let _ = insert_records(&mut transaction, records.clone())
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let mut query_params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: String::from("mbp-1"),
        };

        let mut cursor =
            Mbp1Msg::retrieve_query(&pool, &mut query_params)
                .await
                .expect("Error on retrieve records.");

        // Validate
        let mut query:Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = Mbp1Msg::from_row(&row)?;

                    // Convert to RecordEnum and add to encoder
                    let record_enum = RecordEnum::Mbp1(record);
                    query.push(record_enum);
                }
                Err(e) => {
                    error!("Error processing row: {:?}", e);
                    return Err(e.into());
                }
            }      
        }

        assert_eq!(records.len(), query.len());

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        Instrument::delete_instrument(&mut transaction, instrument_id)
            .await
            .expect("Error on delete.");

        let _ = transaction.commit().await;
        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_tbbo()-> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let instrument_id = create_instrument(&pool)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564) },
                price: 6770,
                size: 1,
                action: Action::Trade as c_char,
                side: Side::Bid as c_char,
                depth: 0,
                flags: 0,
                ts_recv: 1704209103644092564,
                ts_in_delta: 17493,
                sequence: 739763,
                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092565) },
                price: 6870,
                size: 2,
                action: Action::Add as c_char,
                side: Side::Bid as c_char,
                depth: 0,
                flags: 0,
                ts_recv: 1704209103644092565,
                ts_in_delta: 17493,
                sequence: 739763,
                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
        ];

        let _ = insert_records(&mut transaction, records.clone())
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let mut query_params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: String::from("tbbo"),
        };

        let mut cursor =
            TbboMsg::retrieve_query(&pool, &mut query_params)
                .await
                .expect("Error on retrieve records.");

        // Validate
        let mut query:Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = TbboMsg::from_row(&row)?;

                    // Convert to RecordEnum and add to encoder
                    let record_enum = RecordEnum::Tbbo(record);
                    query.push(record_enum);
                }
                Err(e) => {
                    error!("Error processing row: {:?}", e);
                    return Err(e.into());
                }
            
            }      
        }

        assert_eq!(query.len(), 1);

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        Instrument::delete_instrument(&mut transaction, instrument_id)
            .await
            .expect("Error on delete.");

        let _ = transaction.commit().await;
        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_trade() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let instrument_id = create_instrument(&pool)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564) },
                price: 6770,
                size: 1,
                action: Action::Trade as c_char,
                side: Side::Bid as c_char,
                depth: 0,
                flags: 0,
                ts_recv: 1704209103644092564,
                ts_in_delta: 17493,
                sequence: 739763,
                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092565) },
                price: 6870,
                size: 2,
                action: Action::Trade as c_char,
                side: Side::Bid as c_char,
                depth: 0,
                flags: 0,
                ts_recv: 1704209103644092565,
                ts_in_delta: 17493,
                sequence: 739763,
                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
        ];

        let _ = insert_records(&mut transaction, records.clone())
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let mut query_params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: String::from("trade"),
        };


        let mut cursor =
            TradeMsg::retrieve_query(&pool, &mut query_params)
                .await
                .expect("Error on retrieve records.");

        // Validate
        let mut query:Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = TradeMsg::from_row(&row)?;

                    // Convert to RecordEnum and add to encoder
                    let record_enum = RecordEnum::Trade(record);
                    query.push(record_enum);
                }
                Err(e) => {
                    error!("Error processing row: {:?}", e);
                    return Err(e.into());
                }
            
            }      
        }

        assert!(query.len() == 2);

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        Instrument::delete_instrument(&mut transaction, instrument_id)
            .await
            .expect("Error on delete.");

        let _ = transaction.commit().await;

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_bbo() -> anyhow::Result<()>  {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let instrument_id = create_instrument(&pool)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564) },
                price: 6770,
                size: 1,
                action: Action::Trade as c_char,
                side: Side::Bid as c_char,
                depth: 0,
                flags: 0,
                ts_recv: 1704209103644092564,
                ts_in_delta: 17493,
                sequence: 739763,
                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092565) },
                price: 6870,
                size: 2,
                action: Action::Trade as c_char,
                side: Side::Bid as c_char,
                depth: 0,
                flags: 0,
                ts_recv: 1704209103644092565,
                ts_in_delta: 17493,
                sequence: 739763,
                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
        ];

        let _ = insert_records(&mut transaction, records)
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let mut query_params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092567,
            schema: String::from("bbo-1s"),
        };

        let mut cursor =
            BboMsg::retrieve_query(&pool, &mut query_params)
                .await
                .expect("Error on retrieve records.");

        // Validate
        let mut query:Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = BboMsg::from_row(&row)?;

                    // Convert to RecordEnum and add to encoder
                    let record_enum = RecordEnum::Bbo(record);
                    query.push(record_enum);
                }
                Err(e) => {
                    error!("Error processing row: {:?}", e);
                    return Err(e.into());
                }
            
            }      
        }

        assert!(query.len() > 0);

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        Instrument::delete_instrument(&mut transaction, instrument_id)
            .await
            .expect("Error on delete.");

        let _ = transaction.commit().await;
        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_retrieve_ohlcv() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let instrument_id = create_instrument(&pool)
            .await
            .expect("Error creating instrument.");

        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092562) },
                price: 500,
                size: 1,
                action: Action::Trade as c_char,
                side: Side::Bid as c_char,
                depth: 0,
                flags: 0,
                ts_recv: 1704209103644092562,
                ts_in_delta: 17493,
                sequence: 739763,
                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564) },
                price: 6770,
                size: 1,
                action: Action::Trade as c_char,
                side: 2,
                depth: 0,
                flags: 0,
                ts_recv: 1704209104645092564,
                ts_in_delta: 17493,
                sequence: 739763,
                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704295503644092562) },
                price: 6870,
                size: 2,
                action: Action::Trade as c_char,
                side: 2,
                depth: 0,
                flags: 0,
                ts_recv: 1704295503644092562,
                ts_in_delta: 17493,
                sequence: 739763,
                levels: [BidAskPair {
                    bid_px: 1,
                    ask_px: 1,
                    bid_sz: 1,
                    ask_sz: 1,
                    bid_ct: 10,
                    ask_ct: 20,
                }],
            },
        ];

        let _ = insert_records(&mut transaction, records)
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Test
        let mut query_params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092562,
            end_ts: 1704295503654092563,
            schema: String::from("ohlcv-1d"),
        };
        
        let mut cursor =
           OhlcvMsg::retrieve_query(&pool, &mut query_params)
                .await
                .expect("Error on retrieve records.");

        // Validate
        let mut query:Vec<RecordEnum> = vec![];
        while let Some(row_result) = cursor.next().await {
            match row_result {
                Ok(row) => {
                    let record = OhlcvMsg::from_row(&row)?;

                    // Convert to RecordEnum and add to encoder
                    let record_enum = RecordEnum::Ohlcv(record);
                    query.push(record_enum);
                }
                Err(e) => {
                    error!("Error processing row: {:?}", e);
                    return Err(e.into());
                }
            
            }      
        }

        // Validate
        assert!(query.len() > 0);

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        Instrument::delete_instrument(&mut transaction, instrument_id)
            .await
            .expect("Error on delete.");

        let _ = transaction.commit().await;
        Ok(())
    }
}
