mod heap;
mod mutex_cursor;
mod query_task;
pub mod retriever;

use crate::Result;
use axum::response::IntoResponse;
use axum::{body::Body, routing::get, Router};
use axum::{Extension, Json};
use mbinary::params::RetrieveParams;
use retriever::RecordGetter;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::info;

// Service
pub fn get_service() -> Router {
    Router::new().route("/stream", get(get_records))
}

pub async fn get_records(
    Extension(pool): Extension<PgPool>,
    Json(params): Json<RetrieveParams>,
) -> Result<impl IntoResponse> {
    info!(
        "Retrieving {:?} records for symbols: {:?} start: {:?} end: {:?} ",
        params.schema, params.symbols, params.start_ts, params.end_ts
    );

    // Initialize the loader
    let loader = Arc::new(RecordGetter::new(1000000, params, pool).await?);
    let progress_stream = loader.stream().await;

    Ok(Body::from_stream(progress_stream))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::database::init::init_db;
    use crate::response::ApiResponse;
    use crate::services::load::create_record;
    use axum::response::IntoResponse;
    use axum::{Extension, Json};
    use futures::stream::StreamExt;
    use mbinary::encode::CombinedEncoder;
    use mbinary::enums::Dataset;
    use mbinary::metadata::Metadata;
    use mbinary::record_enum::RecordEnum;
    use mbinary::record_ref::RecordRef;
    use mbinary::symbols::SymbolMap;
    use mbinary::vendors::Vendors;
    use mbinary::vendors::{DatabentoData, VendorData};
    use mbinary::{
        decode::Decoder,
        enums::{Schema, Stype},
        records::{BidAskPair, Mbp1Msg, RecordHeader},
        symbols::Instrument,
    };
    use serial_test::serial;
    use sqlx::postgres::PgPoolOptions;
    use std::io::Cursor;
    use std::str::FromStr;

    // -- Helper functions
    async fn create_instrument(instrument: Instrument) -> Result<i32> {
        let database_url = std::env::var("INSTRUMENT_DATABASE_URL")?;
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&database_url)
            .await?;

        let mut tx = pool.begin().await.expect("Error settign up database.");

        // Insert dataset into the instrument table and fetch the ID
        let instrument_id: i32 = sqlx::query_scalar(
            r#"
            INSERT INTO instrument (dataset)
            VALUES ($1)
            RETURNING id
            "#,
        )
        .bind(instrument.dataset.clone() as i16)
        .fetch_one(&mut *tx) // Borrow tx mutably
        .await?;

        let query = format!(
            r#"
            INSERT INTO {} (instrument_id, ticker, name, vendor, vendor_data, last_available, first_available, expiration_date, active)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id
            "#,
            instrument.dataset.as_str()
        );

        let _ = sqlx::query(&query)
            .bind(instrument_id)
            .bind(instrument.ticker)
            .bind(instrument.name)
            .bind(instrument.vendor.as_str())
            .bind(instrument.vendor_data as i64)
            .bind(instrument.last_available as i64)
            .bind(instrument.first_available as i64)
            .bind(instrument.expiration_date as i64)
            .bind(instrument.active)
            .execute(&mut *tx) // Borrow tx mutably
            .await?;

        let _ = tx.commit().await;

        Ok(instrument_id)
    }

    async fn delete_instrument(id: i32) -> Result<()> {
        let database_url = std::env::var("INSTRUMENT_DATABASE_URL")?;
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&database_url)
            .await?;

        let mut tx = pool.begin().await.expect("Error settign up database.");

        let _ = sqlx::query(
            r#"
            DELETE FROM instrument WHERE id = $1
            "#,
        )
        .bind(id)
        .execute(&mut *tx)
        .await?;
        let _ = tx.commit().await;

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_get_record_continuous() -> anyhow::Result<()> {
        dotenv::dotenv().ok();

        let pool = init_db().await.unwrap();
        let transaction = pool.begin().await.expect("Error settign up database.");

        // Create instrument
        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Futures;
        let ticker = "PPJ4";
        let name = "test";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            dataset.clone(),
            Vendors::Databento,
            vendor_data.encode(),
            1676397660000000000,
            1704085200000000000,
            1707933660000000000,
            true,
        );

        let id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");
        let _ = transaction.commit().await;

        // Records
        let mbp_1 = Mbp1Msg {
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1707930000000000001, 0) },
            price: 6770,
            size: 1,
            action: 84,
            side: 2,
            depth: 0,
            flags: 10,
            ts_recv: 1707930000000000001,
            ts_in_delta: 17493,
            sequence: 739763,
            discriminator: 0,
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
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1707933650000000000, 0) },
            price: 6870,
            size: 2,
            action: 84,
            side: 1,
            depth: 0,
            flags: 0,
            ts_recv: 1707933650000000000,
            ts_in_delta: 17493,
            sequence: 739763,
            discriminator: 0,
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

        let metadata = Metadata::new(
            Schema::Mbp1,
            dataset,
            1704209103644092564,
            1704209103644092566,
            SymbolMap::new(),
        );

        let mut buffer = Vec::new();
        let mut encoder = CombinedEncoder::new(&mut buffer);
        encoder.encode_metadata(&metadata)?;
        encoder
            .encode_records(&[record_ref1, record_ref2])
            .expect("Encoding failed");

        let response = create_record(Extension(pool.clone()), Json(buffer))
            .await
            .expect("Error creating records.")
            .into_response();

        let mut stream = response.into_body().into_data_stream();

        // Collect streamed responses
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    let bytes_str = String::from_utf8_lossy(&bytes);

                    match serde_json::from_str::<ApiResponse<String>>(&bytes_str) {
                        Ok(response) => {
                            // println!("{:?}", response);
                            if response.status == "success" {}
                        }
                        Err(e) => {
                            eprintln!("Failed to parse chunk: {:?}, raw chunk: {}", e, bytes_str);
                        }
                    }
                }
                Err(e) => {
                    panic!("Error while reading chunk: {:?}", e);
                }
            }
        }

        // Test
        let params = RetrieveParams {
            symbols: vec!["PP.c.0".to_string()],
            start_ts: 1672549200000000000,
            end_ts: 1747242000000000000,
            schema: Schema::Mbp1,
            dataset,
            stype: Stype::Continuous,
        };

        let response = get_records(Extension(pool.clone()), Json(params))
            .await
            .into_response();

        let mut body = response.into_body().into_data_stream();

        // Collect streamed response
        let mut buffer = Vec::new();
        while let Some(chunk) = body.next().await {
            match chunk {
                Ok(bytes) => {
                    // println!("{:?}", bytes);
                    buffer.extend_from_slice(&bytes);
                }
                Err(e) => panic!("Error while reading chunk: {:?}", e),
            }
        }

        let cursor = Cursor::new(buffer);
        let mut decoder = Decoder::new(cursor)?;
        let _decoded_metadata = decoder.metadata().unwrap();
        let records = decoder.decode()?;

        // Validate
        assert!(!records.is_empty(), "Streamed data should not be empty");

        // Cleanup
        delete_instrument(id).await.expect("Error on delete");

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_get_records() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let transaction = pool.begin().await.expect("Error settign up database.");

        // Create instrument
        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("XNAS.ITCH")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Equities;
        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            dataset.clone(),
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            true,
        );

        let id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");
        let _ = transaction.commit().await;

        // Records
        let mbp_1 = Mbp1Msg {
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564, 0) },
            price: 6770,
            size: 1,
            action: 1,
            side: 2,
            depth: 0,
            flags: 10,
            ts_recv: 1704209103644092564,
            ts_in_delta: 17493,
            sequence: 739763,
            discriminator: 0,
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
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092565, 0) },
            price: 6870,
            size: 2,
            action: 1,
            side: 1,
            depth: 0,
            flags: 0,
            ts_recv: 1704209103644092565,
            ts_in_delta: 17493,
            sequence: 739763,
            discriminator: 0,
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

        let metadata = Metadata::new(
            Schema::Mbp1,
            dataset,
            1704209103644092564,
            1704209103644092566,
            SymbolMap::new(),
        );

        let mut buffer = Vec::new();
        let mut encoder = CombinedEncoder::new(&mut buffer);
        encoder.encode_metadata(&metadata)?;
        encoder
            .encode_records(&[record_ref1, record_ref2])
            .expect("Encoding failed");

        let response = create_record(Extension(pool.clone()), Json(buffer))
            .await
            .expect("Error creating records.")
            .into_response();
        let mut stream = response.into_body().into_data_stream();

        // Collect streamed responses
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    let bytes_str = String::from_utf8_lossy(&bytes);

                    match serde_json::from_str::<ApiResponse<String>>(&bytes_str) {
                        Ok(response) => if response.status == "success" {},
                        Err(e) => {
                            eprintln!("Failed to parse chunk: {:?}, raw chunk: {}", e, bytes_str);
                        }
                    }
                }
                Err(e) => {
                    panic!("Error while reading chunk: {:?}", e);
                }
            }
        }

        // Test
        let params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092569,
            schema: Schema::Mbp1,
            dataset,
            stype: Stype::Raw,
        };

        let response = get_records(Extension(pool.clone()), Json(params))
            .await
            .into_response();

        let mut body = response.into_body().into_data_stream();

        // Collect streamed response
        let mut buffer = Vec::new();
        while let Some(chunk) = body.next().await {
            match chunk {
                Ok(bytes) => buffer.extend_from_slice(&bytes),
                Err(e) => panic!("Error while reading chunk: {:?}", e),
            }
        }

        let cursor = Cursor::new(buffer);
        let mut decoder = Decoder::new(cursor)?;
        let records = decoder.decode()?; //.expect("Error decoding metadata.");

        // Validate
        assert_eq!(RecordEnum::Mbp1(mbp_1), records[0]);
        assert_eq!(RecordEnum::Mbp1(mbp_2), records[1]);
        assert!(!records.is_empty(), "Streamed data should not be empty");

        // Cleanup
        delete_instrument(id).await.expect("Error on delete");

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_get_record_no_records() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();
        let transaction = pool.begin().await.expect("Error settign up database.");

        // Create instrument
        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("XNAS.ITCH")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Equities;
        let ticker = "AAPL";
        let name = "Apple Inc.";
        let instrument = Instrument::new(
            None,
            ticker,
            name,
            dataset.clone(),
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            true,
        );

        let id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");

        let _ = transaction.commit().await;

        // Test
        let params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092569,
            schema: Schema::Mbp1,
            dataset,
            stype: Stype::Raw,
        };

        let response = get_records(Extension(pool.clone()), Json(params))
            .await
            .into_response();

        let mut body = response.into_body().into_data_stream();

        // Collect streamed response
        let mut buffer = Vec::new();
        while let Some(chunk) = body.next().await {
            match chunk {
                Ok(bytes) => buffer.extend_from_slice(&bytes),
                Err(e) => panic!("Error while reading chunk: {:?}", e),
            }
        }

        let cursor = Cursor::new(buffer);
        let mut decoder = Decoder::new(cursor)?;
        let records = decoder.decode()?; //.expect("Error decoding metadata.");

        // Validate
        let symbols_map = decoder.metadata.unwrap().mappings;
        let instrument_id = symbols_map.map.get(&(id as u32)); //("AAPL");
        assert_eq!(instrument_id.unwrap(), &"AAPL".to_string());
        assert!(records.len() == 0);

        // Cleanup
        delete_instrument(id).await.expect("Error on delete");

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_get_record_no_records_no_ticker() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        // Test
        let params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092563,
            end_ts: 1704209903644092569,
            schema: Schema::Mbp1,
            dataset: Dataset::Equities,
            stype: Stype::Raw,
        };

        let response = get_records(Extension(pool.clone()), Json(params))
            .await
            .into_response();

        let mut body = response.into_body().into_data_stream();

        // Collect streamed response
        let mut buffer = Vec::new();
        while let Some(chunk) = body.next().await {
            match chunk {
                Ok(bytes) => buffer.extend_from_slice(&bytes),
                Err(e) => panic!("Error while reading chunk: {:?}", e),
            }
        }

        let cursor = Cursor::new(buffer);
        let mut decoder = Decoder::new(cursor)?;
        let records = decoder.decode()?; //.expect("Error decoding metadata.");

        // Validate
        assert!(records.len() == 0);

        Ok(())
    }
}
