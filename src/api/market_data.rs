use crate::database::market_data::{RecordInsertQueries, RecordRetrieveQueries, RetrieveParams};
use crate::mbn::record_enum::RecordEnum;
use crate::mbn::record_ref::RecordRef;
use crate::response::ApiResponse;
use crate::Result;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use mbn::decode::RecordDecoder;
use mbn::encode::CombinedEncoder;
use mbn::enums::Schema;
use mbn::metadata::Metadata;
use sqlx::PgPool;
use std::io::Cursor;
use std::str::FromStr;

// Service
pub fn market_data_service() -> Router {
    Router::new()
        .route("/create", post(create_record))
        .route("/get", get(get_records))
}

// Handlers
pub async fn create_record(
    Extension(pool): Extension<PgPool>,
    Json(encoded_data): Json<Vec<u8>>,
) -> Result<ApiResponse<()>> {
    // Decode received binary data
    let cursor = Cursor::new(encoded_data);
    let mut decoder = RecordDecoder::new(cursor);
    let records = decoder.decode_to_owned()?;

    // Insert decode records
    let mut tx = pool.begin().await?;
    for record in records {
        match record {
            RecordEnum::Mbp1(record) => {
                record.insert_query(&mut tx).await?;
            }
            // Add cases for other record types if needed
            _ => unimplemented!(),
        }
    }
    tx.commit().await?;

    Ok(ApiResponse::new(
        "success",
        "Successfully inserted records.",
        StatusCode::OK,
        None,
    ))
}

pub async fn get_records(
    Extension(pool): Extension<PgPool>,
    Json(params): Json<RetrieveParams>,
) -> Result<ApiResponse<Vec<u8>>> {
    // Retrieve Records
    let (records, map) = RecordEnum::retrieve_query(&pool, &params).await?;
    let record_refs: Vec<RecordRef> = records
        .iter()
        .map(|record| record.to_record_ref())
        .collect();

    // Encode Records
    // let mut buffer = Vec::new();
    // let mut encoder = RecordEncoder::new(&mut buffer);
    // encoder.encode_records(&record_refs)?;
    // // Symbols map
    // let map = SymbolMap::new();
    // map.add_instrument(ticker, id);

    // Metadata
    let metadata = Metadata::new(
        Schema::from_str(&params.schema)?,
        params.start_ts as u64,
        params.end_ts as u64,
        map,
    );

    // Encode Metadata & Records
    let mut buffer = Vec::new();
    let mut encoder = CombinedEncoder::new(&mut buffer);
    encoder.encode_metadata_and_records(&metadata, &record_refs)?;

    Ok(ApiResponse::new(
        "success",
        "",
        StatusCode::OK,
        Some(buffer),
    ))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::database::init::init_quest_db;
    use crate::database::symbols::InstrumentsQueries;
    use crate::mbn::encode::RecordEncoder;
    use crate::mbn::{
        enums::Schema,
        records::{BidAskPair, Mbp1Msg, RecordHeader},
        symbols::Instrument,
    };

    use serial_test::serial;

    #[sqlx::test]
    #[serial]
    async fn test_create_records() {
        dotenv::dotenv().ok();
        let pool = init_quest_db().await.unwrap();
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        // Create instrument
        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(ticker, name, None);
        let id: i32 = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        let _ = transaction.commit().await;

        // Records
        let mbp_1 = Mbp1Msg {
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564) },
            price: 6770,
            size: 1,
            action: 1,
            side: 2,
            depth: 0,
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
        };
        let mbp_2 = Mbp1Msg {
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564) },
            price: 6870,
            size: 2,
            action: 1,
            side: 1,
            depth: 0,
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
        };

        // Test
        let record_ref1: RecordRef = (&mbp_1).into();
        let record_ref2: RecordRef = (&mbp_2).into();

        let mut buffer = Vec::new();
        let mut encoder = RecordEncoder::new(&mut buffer);
        encoder
            .encode_records(&[record_ref1, record_ref2])
            .expect("Encoding failed");

        let result = create_record(Extension(pool.clone()), Json(buffer))
            .await
            .expect("Error creating records.");

        // Validate
        assert_eq!(result.code, StatusCode::OK);

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");
        Instrument::delete_instrument(&mut transaction, id)
            .await
            .expect("Error on delete.");
        let _ = transaction.commit().await;
    }

    #[sqlx::test]
    #[serial]
    async fn test_get_record() {
        dotenv::dotenv().ok();
        let pool = init_quest_db().await.unwrap();
        let mut transaction = pool.begin().await.expect("Error settign up database.");

        // Create instrument
        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(ticker, name, None);
        let id: i32 = instrument
            .insert_instrument(&mut transaction)
            .await
            .expect("Error inserting symbol.");
        let _ = transaction.commit().await;

        // Records
        let mbp_1 = Mbp1Msg {
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564) },
            price: 6770,
            size: 1,
            action: 1,
            side: 2,
            depth: 0,
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
        };
        let mbp_2 = Mbp1Msg {
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564) },
            price: 6870,
            size: 2,
            action: 1,
            side: 1,
            depth: 0,
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
        };

        let record_ref1: RecordRef = (&mbp_1).into();
        let record_ref2: RecordRef = (&mbp_2).into();

        let mut buffer = Vec::new();
        let mut encoder = RecordEncoder::new(&mut buffer);
        encoder
            .encode_records(&[record_ref1, record_ref2])
            .expect("Encoding failed");

        let result = create_record(Extension(pool.clone()), Json(buffer))
            .await
            .expect("Error creating records.");

        assert_eq!(result.code, StatusCode::OK);

        // Test
        let params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092564,
            schema: Schema::Mbp1.to_string(),
        };

        let result = get_records(Extension(pool.clone()), Json(params))
            .await
            .expect("Error on get results")
            .data
            .unwrap();

        // Validate
        // let cursor = Cursor::new(result);
        // let mut decoder = CombinedDecoder::new(cursor);
        // let decoded = decoder.decode_metadata_and_records();
        // println!("{:?}", decoded);
        assert!(result.len() > 0);

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");
        Instrument::delete_instrument(&mut transaction, id)
            .await
            .expect("Error on delete.");
        let _ = transaction.commit().await;
    }
}
