use crate::database::create::{rollback_all_batches, InsertBatch};
use crate::database::utils::get_lastest_mbp_id;
use crate::response::ApiResponse;
use crate::{Error, Result};
use async_stream::stream;
use axum::http::StatusCode;
use bytes::Bytes;
use futures::stream::Stream;
use mbinary::decode::Decoder;
use mbinary::enums::Dataset;
use mbinary::record_enum::RecordEnum;
use sqlx::PgPool;
use std::pin::Pin;
use tracing::error;

pub struct RecordLoader {
    batch_size: usize,
    records_in_batch: usize,
    batch: InsertBatch,
    pool: PgPool,
    last_id: i32,
    dataset: Dataset,
}

impl RecordLoader {
    pub async fn new(batch_size: usize, dataset: Dataset, pool: PgPool) -> Result<Self> {
        let last_id = get_lastest_mbp_id(&pool, &dataset).await?;

        Ok(RecordLoader {
            batch_size,
            records_in_batch: 0,
            batch: InsertBatch::new(dataset.clone()),
            pool,
            last_id,
            dataset,
        })
    }

    pub fn get_records_in_batch(&self) -> usize {
        self.records_in_batch
    }

    pub async fn update_batch(
        &mut self,
        record: RecordEnum,
    ) -> Result<Option<ApiResponse<String>>> {
        match record {
            RecordEnum::Mbp1(msg) => {
                self.batch.process(&msg).await?;
                self.records_in_batch += 1;
            }
            _ => {
                return Err(crate::error!(
                    CustomError,
                    "Invalid record type {:?}",
                    record
                ))
            }
        }

        if self.records_in_batch >= self.batch_size {
            let response = self.commit_batch().await?;
            self.records_in_batch = 0;
            return Ok(Some(response));
        }

        Ok(None)
    }

    pub async fn commit_batch(&mut self) -> Result<ApiResponse<String>> {
        let mut tx = self.pool.begin().await?;
        match self.batch.execute(&mut tx).await {
            Ok(_) => {
                tx.commit().await?;
                let response = ApiResponse::new(
                    "success",
                    &format!("Processed {} records.", self.records_in_batch),
                    StatusCode::OK,
                    "".to_string(),
                );
                Ok(response)
            }
            Err(e) => {
                tx.rollback().await?;
                error!("Error during batch execution: {:?}", e);
                let response = ApiResponse::new(
                    "failed",
                    &format!("Error during batch execution: {:?}", e),
                    StatusCode::CONFLICT,
                    "".to_string(),
                );
                Ok(response)
            }
        }
    }

    pub async fn cleanup(&self) -> Result<ApiResponse<String>> {
        let mut tx = self.pool.begin().await?;
        match rollback_all_batches(self.last_id, &self.dataset, &mut tx).await {
            Ok(_) => {
                tx.commit().await?;
                let response = ApiResponse::new(
                    "success",
                    "Removed all commits from this batch process.",
                    StatusCode::OK,
                    "".to_string(),
                );
                Ok(response)
            }
            Err(e) => {
                tx.rollback().await?;
                error!("Fialed to remove all batched from current process: {:?}", e);
                let response = ApiResponse::new(
                    "failed",
                    &format!("Fialed to remove all batched from current process: {:?}", e),
                    StatusCode::CONFLICT,
                    "".to_string(),
                );
                Ok(response)
            }
        }
    }

    pub async fn process_records<R>(
        mut self,
        mut decoder: Decoder<R>,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>
    where
        R: std::io::Read + Send + 'static,
    {
        // println!("Processign records");
        let p_stream = stream! {
            // First responnse will contain the status of the mbp table before beginning
            let response = ApiResponse::new(
                "success",
                &format!("Max ID before inserting {}", self.last_id),
                StatusCode::OK,
                format!("{}", self.last_id),
            );
            yield Ok(response.bytes());

            // Initialize the decoder
            let mut decode_iter = decoder.decode_iterator();

            while let Some(record_result) = decode_iter.next() {
                // info!("Test {:?}", record_result);
                match record_result {
                    Ok(record) => {
                        match self.update_batch(record).await {
                            Ok(Some(response)) =>{
                                // Yield response as Bytes to the stream
                                yield Ok::<Bytes, Error>(response.bytes());

                                // Break the stream if the response status is not "success"
                                if response.status != "success" {
                                    let c_response : Bytes = self.cleanup().await.unwrap().bytes();
                                    yield Ok(c_response);

                                    return;
                                }
                            }
                            Ok(None) => {}
                            Err(e) => {
                                error!("Error processing record: {:?}", e);
                                yield Ok(e.bytes());

                                let c_response : Bytes = self.cleanup().await.unwrap().bytes();
                                yield Ok(c_response);

                                return;
                            }
                        }
                    },
                    Err(e) => {
                        error!("Error decoding record: {:?}", e);
                        let response = ApiResponse::new(
                            "failed",
                            &format!("Error decoding record: {:?}", e),
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "".to_string(),
                        );
                        yield Ok(response.bytes());

                        let c_response : Bytes = self.cleanup().await.unwrap().bytes();
                        yield Ok(c_response);

                        return;
                    }
                }
            }

            // Final clean-up
            if self.get_records_in_batch() > 0 {
                match self.commit_batch().await {
                    Ok(response) => {
                        // Yield response as Bytes to the stream
                        yield Ok::<Bytes, Error>(response.bytes());

                        // Break the stream if the response status is not "success"
                        if response.status != "success" {
                            let c_response : Bytes = self.cleanup().await.unwrap().bytes();
                            yield Ok(c_response);
                            return;
                        }
                    }
                    Err(e) => {
                        error!("Error processing record: {:?}", e);
                        yield Ok(e.bytes());

                        let c_response : Bytes = self.cleanup().await.unwrap().bytes();
                        yield Ok(c_response);

                        return;
                    }

                }

            }

            // Success response
            let response = ApiResponse::new(
                "success",
                "Successfully inserted records and merged staging.",
                StatusCode::OK,
                "".to_string(),
            );
            yield Ok(response.bytes());

        };
        Box::pin(p_stream) // Explicitly pin the stream and return it
    }
}
