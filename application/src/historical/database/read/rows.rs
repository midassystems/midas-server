use crate::Result;
use mbinary::enums::RType;
use mbinary::record_enum::RecordEnum;
use mbinary::records::{BboMsg, BidAskPair, Mbp1Msg, OhlcvMsg, RecordHeader, TbboMsg, TradeMsg};
use sqlx::Row;
use std::os::raw::c_char;

pub trait FromRow: Sized {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self>;
}

impl FromRow for Mbp1Msg {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self> {
        Ok(Mbp1Msg {
            hd: RecordHeader::new::<Mbp1Msg>(
                row.try_get::<i32, _>("instrument_id")? as u32,
                row.try_get::<i64, _>("ts_event")? as u64,
                row.try_get::<i32, _>("rollover_flag")? as u8,
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
            discriminator: row.try_get::<i32, _>("discriminator")? as u32,
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

impl FromRow for TradeMsg {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self> {
        Ok(TradeMsg {
            hd: RecordHeader::new::<TradeMsg>(
                row.try_get::<i32, _>("instrument_id")? as u32,
                row.try_get::<i64, _>("ts_event")? as u64,
                row.try_get::<i32, _>("rollover_flag")? as u8,
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

impl FromRow for BboMsg {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self> {
        Ok(BboMsg {
            hd: RecordHeader::new::<BboMsg>(
                row.try_get::<i32, _>("instrument_id")? as u32,
                row.try_get::<i64, _>("ts_event")? as u64,
                row.try_get::<i32, _>("rollover_flag")? as u8,
            ),
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

impl FromRow for OhlcvMsg {
    fn from_row(row: &sqlx::postgres::PgRow) -> Result<Self> {
        Ok(OhlcvMsg {
            hd: RecordHeader::new::<OhlcvMsg>(
                row.try_get::<i32, _>("instrument_id")? as u32,
                row.try_get::<i64, _>("ts_event")? as u64,
                row.try_get::<i32, _>("rollover_flag")? as u8,
            ),
            open: row.try_get::<i64, _>("open")?,
            close: row.try_get::<i64, _>("close")?,
            low: row.try_get::<i64, _>("low")?,
            high: row.try_get::<i64, _>("high")?,
            volume: row.try_get::<i64, _>("volume")? as u64,
        })
    }
}

type FromRowFn = fn(&sqlx::postgres::PgRow) -> Result<RecordEnum>;

pub fn get_from_row_fn(rtype: RType) -> FromRowFn {
    match rtype {
        RType::Mbp1 => |row| Ok(RecordEnum::Mbp1(Mbp1Msg::from_row(row)?)),
        RType::Trades => |row| Ok(RecordEnum::Trade(TradeMsg::from_row(row)?)),
        RType::Ohlcv => |row| Ok(RecordEnum::Ohlcv(OhlcvMsg::from_row(row)?)),
        RType::Bbo => |row| Ok(RecordEnum::Bbo(BboMsg::from_row(row)?)),
        RType::Tbbo => |row| Ok(RecordEnum::Tbbo(TbboMsg::from_row(row)?)),
    }
}
