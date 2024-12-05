use crate::Result;
use async_trait::async_trait;
use mbn::records::{BidAskPair, Mbp1Msg};
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Transaction};

// Function to compute a unique hash for the order book state
fn compute_order_book_hash(levels: &[BidAskPair]) -> String {
    let mut hasher = Sha256::new();

    for level in levels {
        hasher.update(level.bid_px.to_be_bytes());
        hasher.update(level.ask_px.to_be_bytes());
        hasher.update(level.bid_sz.to_be_bytes());
        hasher.update(level.ask_sz.to_be_bytes());
        hasher.update(level.bid_ct.to_be_bytes());
        hasher.update(level.ask_ct.to_be_bytes());
    }

    let result = hasher.finalize();

    // Convert hash result to a hex string
    result.iter().map(|byte| format!("{:02x}", byte)).collect()
}


/// Primary insert method
pub struct InsertBatch {
    pub mbp_values: Vec<(i32, i64, i64, i32, i32, i32, i32, i64, i32, i32, String)>,
    pub bid_ask_batches: Vec<Vec<(i32, i64, i32, i32, i64, i32, i32)>>,

}
impl InsertBatch {
    pub fn new() -> Self {
        InsertBatch {
            mbp_values: Vec::new(),
            bid_ask_batches: Vec::new(),
        }
    }

    pub async fn process(&mut self, record: &Mbp1Msg) -> Result<()> {
        // Compute the order book hash based on levels
        let order_book_hash = compute_order_book_hash(&record.levels);

        // Add the current mbp row to the batch
        self.mbp_values.push((
            record.hd.instrument_id as i32,
            record.hd.ts_event as i64,
            record.price,
            record.size as i32,
            record.action as i32,
            record.side as i32,
            record.flags as i32,
            record.ts_recv as i64,
            record.ts_in_delta,
            record.sequence as i32,
            order_book_hash
        ));

        // Collect bid_ask rows associated with this mbp record
        let mut bid_ask_for_mbp: Vec<(i32, i64, i32, i32, i64, i32, i32)> = Vec::new();
        for (depth, level) in record.levels.iter().enumerate() {
            bid_ask_for_mbp.push((
                depth as i32,       // Depth
                level.bid_px,       // Bid price
                level.bid_sz as i32, // Bid size
                level.bid_ct as i32, // Bid count
                level.ask_px,       // Ask price
                level.ask_sz as i32, // Ask size
                level.ask_ct as i32  // Ask count
            ));
        }
        self.bid_ask_batches.push(bid_ask_for_mbp);
        Ok(())
    }

    pub async fn execute(&mut self, tx: &mut Transaction<'_, Postgres>) -> Result<()> {
        // Manually unpack mbp_values into separate vectors
        let mut instrument_ids = Vec::new();
        let mut ts_events = Vec::new();
        let mut prices = Vec::new();
        let mut sizes = Vec::new();
        let mut actions = Vec::new();
        let mut sides = Vec::new();
        let mut flags = Vec::new();
        let mut ts_recvs = Vec::new();
        let mut ts_in_deltas = Vec::new();
        let mut sequences = Vec::new();
        let mut order_book_hashes = Vec::new();

        for (id, ts, price, size, action, side, flag, recv, delta, seq, hash) in &self.mbp_values {
            instrument_ids.push(*id);
            ts_events.push(*ts);
            prices.push(*price);
            sizes.push(*size);
            actions.push(*action);
            sides.push(*side);
            flags.push(*flag);
            ts_recvs.push(*recv);
            ts_in_deltas.push(*delta);
            sequences.push(*seq);
            order_book_hashes.push(hash.as_str()); // Use &str for the string
        }

        // INsert into mbp
        let mbp_ids: Vec<i32> = sqlx::query_scalar(
            r#"
            INSERT INTO mbp (instrument_id, ts_event, price, size, action, side, flags, ts_recv, ts_in_delta, sequence, order_book_hash)
            SELECT * FROM UNNEST($1::int[], $2::bigint[], $3::bigint[], $4::int[], $5::int[], $6::int[], $7::int[], $8::bigint[], $9::int[], $10::int[], $11::text[])
            ON CONFLICT DO NOTHING
            RETURNING id
            "#
        )
        .bind(&instrument_ids)
        .bind(&ts_events)
        .bind(&prices)
        .bind(&sizes)
        .bind(&actions)
        .bind(&sides)
        .bind(&flags)
        .bind(&ts_recvs)
        .bind(&ts_in_deltas)
        .bind(&sequences)
        .bind(&order_book_hashes) 
        .fetch_all(&mut *tx)
        .await?;

        // Create separate vectors for each field
        let mut depths = Vec::new();
        let mut bid_px = Vec::new();
        let mut bid_sz = Vec::new();
        let mut bid_ct = Vec::new();
        let mut ask_px = Vec::new();
        let mut ask_sz = Vec::new();
        let mut ask_ct = Vec::new();

        for (idx, _mbp_id) in mbp_ids.iter().enumerate() {
            let bid_ask_for_current_mbp = &self.bid_ask_batches[idx];


            // Unpack the tuples into their respective vectors
            for (depth, bid_price, bid_size, bid_count, ask_price, ask_size, ask_count) in bid_ask_for_current_mbp {
                depths.push(*depth);
                bid_px.push(*bid_price);
                bid_sz.push(*bid_size);
                bid_ct.push(*bid_count);
                ask_px.push(*ask_price);
                ask_sz.push(*ask_size);
                ask_ct.push(*ask_count);
            }
        }

        // Insert all bid_ask levels associated with this mbp_id
        sqlx::query(
            r#"
            INSERT INTO bid_ask (mbp_id, depth, bid_px, bid_sz, bid_ct, ask_px, ask_sz, ask_ct)
            SELECT * FROM UNNEST($1::int[], $2::int[], $3::bigint[], $4::int[], $5::int[], $6::bigint[], $7::int[], $8::int[])
            "#
        )
        .bind(mbp_ids)
        .bind(&depths)
        .bind(&bid_px)
        .bind(&bid_sz)
        .bind(&bid_ct)
        .bind(&ask_px)
        .bind(&ask_sz)
        .bind(&ask_ct)
        .execute(&mut *tx)
        .await?;

        // Clear the batch after committing
        self.mbp_values.clear();
        self.bid_ask_batches.clear();

        Ok(())
    }

}


// Utility function for testing
#[async_trait]
pub trait RecordInsertQueries {
    async fn insert_query(&self, tx: &mut Transaction<'_, Postgres>) -> Result<()>;
}

/// For smaller inserts
#[async_trait]
impl RecordInsertQueries for Mbp1Msg {
    async fn insert_query(&self, tx: &mut Transaction<'_, Postgres>) -> Result<()> {
        // Compute the order book hash based on levels
        let order_book_hash = compute_order_book_hash(&self.levels);

        // Insert into mbp table
        let mbp_id: i32 = sqlx::query_scalar(
            r#"
            INSERT INTO mbp (instrument_id, ts_event, price, size, action, side,flags, ts_recv, ts_in_delta, sequence, order_book_hash)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            RETURNING id
            "#
        )
        .bind(self.hd.instrument_id as i32)
        .bind(self.hd.ts_event as i64)
        .bind(self.price)
        .bind(self.size as i32)
        .bind(self.action as i32)
        .bind(self.side as i32)
        .bind(self.flags as i32)
        .bind(self.ts_recv as i64)
        .bind(self.ts_in_delta)
        .bind(self.sequence as i32)
        .bind(&order_book_hash) // Bind the computed hash
        .fetch_one(&mut *tx)
        .await?;
     
        // Insert into bid_ask table
        for (depth, level) in self.levels.iter().enumerate() {
            let _ = sqlx::query(
                r#"
                INSERT INTO bid_ask (mbp_id, depth, bid_px, bid_sz, bid_ct, ask_px, ask_sz, ask_ct)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                "#,
            )
            .bind(mbp_id)
            .bind(depth as i32) // Using the index as depth
            .bind(level.bid_px)
            .bind(level.bid_sz as i32)
            .bind(level.bid_ct as i32)
            .bind(level.ask_px)
            .bind(level.ask_sz as i32)
            .bind(level.ask_ct as i32)
            .execute(&mut *tx)
            .await?;
        }

        Ok(())
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use crate::database::init::init_db;
    use crate::database::symbols::*;
    use crate::services::market_data::retrieve::get_records;
    use mbn::enums::{Action, Side};
    use mbn::record_enum::RecordEnum;
    use mbn::symbols::Instrument;
    use serial_test::serial;
    use mbn::symbols::Vendors;
    use sqlx::{PgPool, Postgres, Transaction};
    use mbn::records::{BidAskPair, Mbp1Msg, RecordHeader};
    use std::os::raw::c_char;
    use crate::database::market_data::read::RetrieveParams;
    use mbn::enums::Schema;
    use axum::response::IntoResponse;
    use axum::{Extension, Json}; 
    use mbn::decode::Decoder;
    use std::io::Cursor;
    use hyper::body::HttpBody as _;
    use mbn::record_ref::RecordRef;
    use mbn::encode::RecordEncoder;
    use std::path::PathBuf;
    use std::fs::File;
    use std::io::BufReader;
    use mbn::decode::RecordDecoder;

    // -- Helper functions 
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
        Ok(())
    }

    async fn insert_batch_records( pool: PgPool, filepath: PathBuf) -> anyhow::Result<()> {
        let mut insert_batch = InsertBatch::new();

        // Create a stream to send updates
        let mut decoder = RecordDecoder::<BufReader<File>>::from_file(&filepath)?;
        let mut decode_iter = decoder.decode_iterator();

        while let Some(record_result) = decode_iter.next() {
            let record_enum = record_result?; 
            match record_enum {
                RecordEnum::Mbp1(msg) => {
                    insert_batch.process(&msg).await?;
                }
                _ => unimplemented!(),
            }
        }
     
        let mut tx = pool.begin().await?;
        insert_batch.execute(&mut tx).await?;
        tx.commit().await?;
    
        Ok(())
    }

    async fn retrieve_records(
        pool : PgPool,
        params: RetrieveParams,
        )  -> anyhow::Result<Vec<RecordEnum>> {


        let response = get_records(Extension(pool), Json(params))
            .await
            .into_response();

        let mut body = response.into_body();

        // Collect streamed response
        let mut buffer = Vec::new();
        while let Some(chunk) = body.data().await {
            match chunk {
                Ok(bytes) => buffer.extend_from_slice(&bytes),
                Err(e) => panic!("Error while reading chunk: {:?}", e),
            }
        }

        let cursor = Cursor::new(buffer);
        let mut decoder = Decoder::new(cursor)?;
        let records = decoder.decode()?;

        Ok(records)
    
    }
     
    #[sqlx::test]
    #[serial]
    #[ignore]
    async fn test_create_record() {
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704295503644092562) },
                price: 6870,
                size: 2,
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
        ];

        // Test
        let result = insert_records(&mut transaction, records)
            .await
            .expect("Error inserting records.");

        // Validate
        assert_eq!(result, ());

        // Cleanup
        Instrument::delete_instrument(&mut transaction, instrument_id)
            .await
            .expect("Error on delete.");

        let _ = transaction.commit().await;
    }

    #[sqlx::test]
    #[serial]
    #[ignore]
    async fn test_insert_batch() ->anyhow::Result<()> {
        dotenv::dotenv().ok();
        let pool = init_db().await.unwrap();

        let id = create_instrument(&pool)
            .await
            .expect("Error creating instrument.");

        let _transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

         // Records
        let mbp_1 = Mbp1Msg {
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564) },
            price: 6770,
            size: 1,
            action: 1,
            side: 2,
            depth: 0,
            flags: 130,
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
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092566) },
            price: 6870,
            size: 2,
            action: 1,
            side: 1,
            depth: 0,
            flags: 0,
            ts_recv: 1704209103644092566,
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

        let file = "tests/data/test_bulk_upload.bin";
        let path = PathBuf::from(file);
        let _ = encoder.write_to_file(&path);

        // Test
        let result = insert_batch_records(pool.clone(), path.clone()).await?;

        // Validate
        assert_eq!(result, ());

        // Cleanup
        let mut transaction = pool
            .begin()
            .await
            .expect("Error setting up test transaction.");
        Instrument::delete_instrument(&mut transaction, id)
            .await
            .expect("Error on delete.");
        let _ = transaction.commit().await;

        // Cleanup
        if path.exists() {
            std::fs::remove_file(&path).expect("Failed to delete the test file.");
        }

        Ok(())

    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_create_duplicate() ->anyhow::Result<()> {
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704295503644092562) },
                price: 6870,
                size: 2,
                action: Action::Add as c_char,
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
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704295503644092562) },
                price: 6870,
                size: 2,
                action: Action::Add as c_char,
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
        ];

        let mut insert_batch = InsertBatch::new();

        for record in &records {
            insert_batch.process(record).await?;
        }
        
        insert_batch.execute(&mut transaction).await?;
        transaction.commit().await?;

        // let _result = insert_records(&mut transaction, records.clone())
        //     .await
        //     .expect("Error inserting records.");
        // transaction.commit().await?;


        // Test
        // let mut transaction = pool
        //     .begin()
        //     .await
        //     .expect("Error setting up test transaction.");
        // let _ = validate_staging(&mut transaction).await?;
        // transaction.commit().await?;

        // let mut transaction = pool
        //     .begin()
        //     .await
        //     .expect("Error setting up test transaction.");
        // let _ = clear_staging(&mut transaction).await?;
        // transaction.commit().await?;

        // Validate 
        let params = RetrieveParams {
            symbols: vec!["AAPL".to_string()],
            start_ts: 1704209103644092561,
            end_ts: 1704209903644092569,
            schema: Schema::Mbp1.to_string(),
        };
        let ret_records = retrieve_records(pool.clone(), params).await?;

        assert_eq!(ret_records.len(), 1);

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

