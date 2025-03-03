use crate::Result;
use async_trait::async_trait;
use mbinary::{
    enums::Dataset,
    records::{BidAskPair, Mbp1Msg},
};
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Transaction};
use std::ops::DerefMut;

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

pub async fn rollback_all_batches(
    last_id: i32,
    dataset: &Dataset,
    tx: &mut Transaction<'_, Postgres>,
) -> Result<()> {
    // info!("Rolling back to {}", last_id);
    let query = format!("DELETE FROM {}_mbp WHERE id > $1", dataset.as_str());
    sqlx::query(&query)
        .bind(last_id)
        .execute(tx.deref_mut())
        .await?;

    Ok(())
}

/// Primary insert method
pub struct InsertBatch {
    pub dataset: Dataset,
    pub mbp_values: Vec<(
        i32,
        i64,
        i64,
        i32,
        i32,
        i32,
        i32,
        i64,
        i32,
        i32,
        i32,
        String,
    )>,
    pub bid_ask_batches: Vec<Vec<(i32, i64, i32, i32, i64, i32, i32)>>,
}
impl InsertBatch {
    pub fn new(dataset: Dataset) -> Self {
        InsertBatch {
            dataset,
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
            record.discriminator as i32,
            order_book_hash,
        ));

        // Collect bid_ask rows associated with this mbp record
        let mut bid_ask_for_mbp: Vec<(i32, i64, i32, i32, i64, i32, i32)> = Vec::new();
        for (depth, level) in record.levels.iter().enumerate() {
            bid_ask_for_mbp.push((
                depth as i32,        // Depth
                level.bid_px,        // Bid price
                level.bid_sz as i32, // Bid size
                level.bid_ct as i32, // Bid count
                level.ask_px,        // Ask price
                level.ask_sz as i32, // Ask size
                level.ask_ct as i32, // Ask count
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
        let mut discriminators = Vec::new();
        let mut order_book_hashes = Vec::new();

        for (id, ts, price, size, action, side, flag, recv, delta, seq, discrim, hash) in
            &self.mbp_values
        {
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
            discriminators.push(*discrim);
            order_book_hashes.push(hash.as_str()); // Use &str for the string
        }

        // INsert into mbp
        let query = format!(
            r#"
            INSERT INTO {}_mbp (instrument_id, ts_event, price, size, action, side, flags, ts_recv, ts_in_delta, sequence, discriminator, order_book_hash)
            SELECT * FROM UNNEST($1::int[], $2::bigint[], $3::bigint[], $4::int[], $5::int[], $6::int[], $7::int[], $8::bigint[], $9::int[], $10::int[], $11::int[], $12::text[])
            RETURNING id
            "#,
            self.dataset.as_str()
        );

        // ON CONFLICT DO NOT/* H */ING
        let mbp_ids: Vec<i32> = sqlx::query_scalar(&query)
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
            .bind(&discriminators)
            .bind(&order_book_hashes)
            .fetch_all(&mut **tx)
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
            for (depth, bid_price, bid_size, bid_count, ask_price, ask_size, ask_count) in
                bid_ask_for_current_mbp
            {
                depths.push(*depth);
                bid_px.push(*bid_price);
                bid_sz.push(*bid_size);
                bid_ct.push(*bid_count);
                ask_px.push(*ask_price);
                ask_sz.push(*ask_size);
                ask_ct.push(*ask_count);
            }
        }

        let query = format!(
            r#"
            INSERT INTO {}_bid_ask (mbp_id, depth, bid_px, bid_sz, bid_ct, ask_px, ask_sz, ask_ct)
            SELECT * FROM UNNEST($1::int[], $2::int[], $3::bigint[], $4::int[], $5::int[], $6::bigint[], $7::int[], $8::int[])
            "#,
            self.dataset.as_str()
        );

        sqlx::query(&query)
            .bind(mbp_ids)
            .bind(&depths)
            .bind(&bid_px)
            .bind(&bid_sz)
            .bind(&bid_ct)
            .bind(&ask_px)
            .bind(&ask_sz)
            .bind(&ask_ct)
            .execute(&mut **tx)
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
    async fn insert_query(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        dataset: &Dataset,
    ) -> Result<()>;
}

/// For smaller inserts
#[async_trait]
impl RecordInsertQueries for Mbp1Msg {
    async fn insert_query(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        dataset: &Dataset,
    ) -> Result<()> {
        // Compute the order book hash based on levels
        let order_book_hash = compute_order_book_hash(&self.levels);

        let query = format!(
            r#"
            INSERT INTO {}_mbp (instrument_id, ts_event, price, size, action, side,flags, ts_recv, ts_in_delta, sequence, discriminator, order_book_hash)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            RETURNING id
            "#,
            dataset.as_str(),
        );

        let mbp_id: i32 = sqlx::query_scalar(&query)
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
            .bind(self.discriminator as i32)
            .bind(&order_book_hash) // Bind the computed hash
            .fetch_one(tx.deref_mut())
            .await?;

        let query = format!(
            r#"
                INSERT INTO {}_bid_ask (mbp_id, depth, bid_px, bid_sz, bid_ct, ask_px, ask_sz, ask_ct)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                "#,
            dataset.as_str()
        );

        // Insert into bid_ask table
        for (depth, level) in self.levels.iter().enumerate() {
            let _ = sqlx::query(&query)
                .bind(mbp_id)
                .bind(depth as i32) // Using the index as depth
                .bind(level.bid_px)
                .bind(level.bid_sz as i32)
                .bind(level.bid_ct as i32)
                .bind(level.ask_px)
                .bind(level.ask_sz as i32)
                .bind(level.ask_ct as i32)
                .execute(tx.deref_mut())
                .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::pool::DatabaseState;
    use mbinary::decode::RecordDecoder;
    use mbinary::encode::RecordEncoder;
    use mbinary::enums::{Action, Dataset, Side};
    use mbinary::record_enum::RecordEnum;
    use mbinary::record_ref::RecordRef;
    use mbinary::records::{BidAskPair, Mbp1Msg, RecordHeader};
    use mbinary::symbols::Instrument;
    use mbinary::vendors::Vendors;
    use mbinary::vendors::{DatabentoData, VendorData};
    use serial_test::serial;
    use sqlx::{postgres::PgPoolOptions, PgPool, Postgres, Transaction};
    use std::fs::File;
    use std::io::BufReader;
    use std::os::raw::c_char;
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::sync::Arc;

    async fn create_db_state() -> anyhow::Result<Arc<DatabaseState>> {
        let historical_db_url = std::env::var("HISTORICAL_DATABASE_URL")?;
        let trading_db_url = std::env::var("TRADING_DATABASE_URL")?;

        let state = DatabaseState::new(&historical_db_url, &trading_db_url).await?;
        Ok(Arc::new(state))
    }
    // -- Helper functions
    async fn create_instrument(instrument: Instrument) -> Result<i32> {
        let database_url = std::env::var("HISTORICAL_DATABASE_URL")?;
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
            INSERT INTO {} (instrument_id, ticker, name, vendor,vendor_data, last_available, first_available, active)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
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
            .bind(instrument.active)
            .execute(&mut *tx) // Borrow tx mutably
            .await?;

        let _ = tx.commit().await;

        Ok(instrument_id)
    }

    async fn delete_instrument(id: i32) -> Result<()> {
        let database_url = std::env::var("HISTORICAL_DATABASE_URL")?;
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

    async fn insert_records(
        tx: &mut Transaction<'_, Postgres>,
        records: Vec<Mbp1Msg>,
        dataset: Dataset,
    ) -> Result<()> {
        for record in records {
            record.insert_query(tx, &dataset).await?;
        }
        Ok(())
    }

    async fn insert_batch_records(
        pool: Arc<PgPool>,
        dataset: Dataset,
        filepath: PathBuf,
    ) -> anyhow::Result<()> {
        let mut insert_batch = InsertBatch::new(dataset);

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

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_create_record() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;
        // let pool = init_db().await.unwrap();

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("XNAS.ITCH")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Equities;
        let instrument = Instrument::new(
            None,
            "AAPL",
            "Apple",
            dataset.clone(),
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            true,
        );

        let instrument_id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");

        let mut transaction = state
            .historical_pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704209103644092564, 0) },
                price: 6770,
                size: 1,
                action: Action::Add as c_char,
                side: Side::Bid as c_char,
                depth: 0,
                flags: 0,
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
            },
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704295503644092562, 0) },
                price: 6870,
                size: 2,
                action: Action::Add as c_char,
                side: Side::Bid as c_char,
                depth: 0,
                flags: 0,
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
            },
        ];

        // Test
        let result = insert_records(&mut transaction, records, dataset)
            .await
            .expect("Error inserting records.");
        let _ = transaction.commit().await;

        // Validate
        assert_eq!(result, ());

        // Cleanup
        delete_instrument(instrument_id)
            .await
            .expect("Error on delete");

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_insert_batch() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Futures;
        let instrument = Instrument::new(
            None,
            "HEJ4",
            "Lean Hogs",
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

        let _transaction = state
            .historical_pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Records
        let mbp_1 = Mbp1Msg {
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092564, 0) },
            price: 6770,
            size: 1,
            action: 1,
            side: 2,
            depth: 0,
            flags: 130,
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
            hd: { RecordHeader::new::<Mbp1Msg>(id as u32, 1704209103644092566, 0) },
            price: 6870,
            size: 2,
            action: 1,
            side: 1,
            depth: 0,
            flags: 0,
            ts_recv: 1704209103644092566,
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

        let mut buffer = Vec::new();
        let mut encoder = RecordEncoder::new(&mut buffer);
        encoder
            .encode_records(&[record_ref1, record_ref2])
            .expect("Encoding failed");

        let file = "tests/data/test_bulk_upload.bin";
        let path = PathBuf::from(file);
        let _ = encoder.write_to_file(&path, false);

        // Test
        let result =
            insert_batch_records(state.historical_pool.clone(), dataset.clone(), path.clone())
                .await?;

        // Validate
        assert_eq!(result, ());

        // Cleanup
        delete_instrument(id).await.expect("Error on delete");

        if path.exists() {
            std::fs::remove_file(&path).expect("Failed to delete the test file.");
        }

        Ok(())
    }

    #[sqlx::test]
    #[serial]
    // #[ignore]
    async fn test_create_duplicate() -> anyhow::Result<()> {
        dotenv::dotenv().ok();
        let state = create_db_state().await?;

        let schema = dbn::Schema::from_str("mbp-1")?;
        let dbn_dataset = dbn::Dataset::from_str("GLBX.MDP3")?;
        let stype = dbn::SType::from_str("raw_symbol")?;
        let vendor_data = VendorData::Databento(DatabentoData {
            schema,
            dataset: dbn_dataset,
            stype,
        });

        let dataset = Dataset::Futures;
        let instrument = Instrument::new(
            None,
            "HEJ4",
            "Lean Hogs",
            dataset.clone(),
            Vendors::Databento,
            vendor_data.encode(),
            1704672000000000000,
            1704672000000000000,
            0,
            true,
        );

        let instrument_id = create_instrument(instrument)
            .await
            .expect("Error creating instrument.");

        let mut transaction = state
            .historical_pool
            .begin()
            .await
            .expect("Error setting up test transaction.");

        // Mock data
        let records = vec![
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704295503644092562, 0) },
                price: 6870,
                size: 2,
                action: Action::Add as c_char,
                side: Side::Bid as c_char,
                depth: 0,
                flags: 0,
                ts_recv: 1704209103644092562,
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
            },
            Mbp1Msg {
                hd: { RecordHeader::new::<Mbp1Msg>(instrument_id as u32, 1704295503644092562, 0) },
                price: 6870,
                size: 2,
                action: Action::Add as c_char,
                side: Side::Bid as c_char,
                depth: 0,
                flags: 0,
                ts_recv: 1704209103644092562,
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
            },
        ];

        let mut insert_batch = InsertBatch::new(dataset.clone());

        for record in &records {
            insert_batch.process(record).await?;
        }

        // Simulate and check for failure
        match insert_batch.execute(&mut transaction).await {
            Ok(_) => {
                panic!("Expected an error, but the operation succeeded!");
            }
            Err(e) => {
                assert!(
                    e.to_string().contains("duplicate key value"),
                    "Unexpected error message: {}",
                    e
                );
            }
        }

        // Ensure transaction is not committed after a failure
        transaction.rollback().await?;

        // Cleanup
        delete_instrument(instrument_id)
            .await
            .expect("Error on delete");

        Ok(())
    }
}
