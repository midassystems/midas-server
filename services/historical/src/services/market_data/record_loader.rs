use crate::database::market_data::create::InsertBatch;
use crate::response::ApiResponse;
use crate::{Error, Result};
use async_stream::stream;
use axum::http::StatusCode;
use bytes::Bytes;
use futures::stream::Stream;
use mbn::decode::RecordDecoder;
use mbn::record_enum::RecordEnum;
use sqlx::PgPool;
use std::pin::Pin;
use tracing::error;

pub struct RecordLoader {
    batch_size: usize,
    records_in_batch: usize,
    batch: InsertBatch,
    pool: PgPool,
}

impl RecordLoader {
    pub fn new(batch_size: usize, pool: PgPool) -> Self {
        RecordLoader {
            batch_size,
            records_in_batch: 0,
            batch: InsertBatch::new(),
            pool,
        }
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

    // pub async fn validate(&self) -> Result<ApiResponse<String>> {
    //     let mut tx = self.pool.begin().await?;
    //     match validate_staging(&mut tx).await {
    //         Ok(_) => {
    //             tx.commit().await?;
    //             let response = ApiResponse::new(
    //                 "success",
    //                 "Merged records into production.",
    //                 StatusCode::OK,
    //                 "".to_string(),
    //             );
    //             Ok(response)
    //         }
    //         Err(e) => {
    //             tx.rollback().await?;
    //             error!("Failed to merge staging into production: {:?}", e);
    //             let response = ApiResponse::new(
    //                 "failed",
    //                 &format!("Failed merge staging into production: {:?}", e),
    //                 StatusCode::CONFLICT,
    //                 "".to_string(),
    //             );
    //             Ok(response)
    //         }
    //     }
    // }

    // /// Handles rollback and clears staging data on errors or cancellations.
    // pub async fn cleanup(&self) -> Result<ApiResponse<String>> {
    //     let mut tx = self.pool.begin().await?;
    //     match clear_staging(&mut tx).await {
    //         Ok(_) => {
    //             tx.commit().await?;
    //             let response = ApiResponse::new(
    //                 "success",
    //                 "Cleared staging sucessfully.",
    //                 StatusCode::OK,
    //                 "".to_string(),
    //             );
    //             Ok(response)
    //         }
    //         Err(e) => {
    //             tx.rollback().await?;
    //             error!("Failed to clear staging: {:?}", e);
    //             let response = ApiResponse::new(
    //                 "failed",
    //                 &format!("Failed to clear staging: {:?}", e),
    //                 StatusCode::CONFLICT,
    //                 "".to_string(),
    //             );
    //             Ok(response)
    //         }
    //     }
    // }

    pub async fn process_records<R>(
        mut self,
        mut decoder: RecordDecoder<R>,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>
    where
        R: std::io::Read + Send + 'static,
    {
        let p_stream = stream! {
            // Initialize the decoder
            let mut decode_iter = decoder.decode_iterator();

            while let Some(record_result) = decode_iter.next() {
                match record_result {
                    Ok(record) => {
                        match self.update_batch(record).await {
                            Ok(Some(response)) =>{
                                // Serialize the response to Bytes
                                let bytes = Bytes::from(serde_json::to_string(&response).unwrap());

                                // Yield the Bytes to the stream
                                yield Ok::<Bytes, Error>(bytes);

                                // Break the stream if the response status is not "success"
                                if response.status != "success" {
                                    return;
                                }
                            }
                            Ok(None) => {} // No response for intermediate records
                            Err(e) => {
                                error!("Error processing record: {:?}", e);
                                let response = ApiResponse::new(
                                    "failed",
                                    &format!("Error processing record: {:?}", e),
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    "".to_string(),
                                );
                                yield Ok(Bytes::from(serde_json::to_string(&response).unwrap()));
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
                        let error_bytes = Bytes::from(serde_json::to_string(&response).unwrap());
                        yield Ok(error_bytes);
                        return;

                    }
                }
            }

            // Final clean-up
            if self.get_records_in_batch() > 0 {
                match self.commit_batch().await {
                    Ok(response) => {
                        // Serialize the response to Bytes
                        let bytes = Bytes::from(serde_json::to_string(&response).unwrap());

                        // Yield the Bytes to the stream
                        yield Ok::<Bytes, Error>(bytes);

                        // Break the stream if the response status is not "success"
                        if response.status != "success" {
                            // let _ = self.cleanup().await?;
                            return;
                        }
                    }
                    Err(e) => {
                        error!("Error processing record: {:?}", e);
                        let response = ApiResponse::new(
                            "failed",
                            &format!("Error processing record: {:?}", e),
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "".to_string(),
                        );
                        yield Ok(Bytes::from(serde_json::to_string(&response).unwrap()));
                        // let _ = self.cleanup().await?;
                        return;
                    }

                }

            }
            // match self.validate().await {
            //         Ok(response) => {
            //             // Serialize the response to Bytes
            //             let bytes = Bytes::from(serde_json::to_string(&response).unwrap());
            //
            //             // Yield the Bytes to the stream
            //             yield Ok::<Bytes, Error>(bytes);
            //
            //             // Break the stream if the response status is not "success"
            //             if response.status != "success" {
            //                 let _ = self.cleanup().await?;
            //                 return;
            //             }
            //         }
            //         Err(e) => {
            //             error!("Error processing record: {:?}", e);
            //             let response = ApiResponse::new(
            //                 "failed",
            //                 &format!("Error processing record: {:?}", e),
            //                 StatusCode::INTERNAL_SERVER_ERROR,
            //                 "".to_string(),
            //             );
            //             yield Ok(Bytes::from(serde_json::to_string(&response).unwrap()));
            //             let _ = self.cleanup().await?;
            //             return;
            //         }
            //
            //     }

            // match self.cleanup().await {
            //         Ok(response) => {
            //             // Serialize the response to Bytes
            //             let bytes = Bytes::from(serde_json::to_string(&response).unwrap());
            //
            //             // Yield the Bytes to the stream
            //             yield Ok::<Bytes, Error>(bytes);
            //
            //             // Break the stream if the response status is not "success"
            //             if response.status != "success" {
            //                 return;
            //             }
            //         }
            //         Err(e) => {
            //             error!("Error processing record: {:?}", e);
            //             let response = ApiResponse::new(
            //                 "failed",
            //                 &format!("Error processing record: {:?}", e),
            //                 StatusCode::INTERNAL_SERVER_ERROR,
            //                 "".to_string(),
            //             );
            //             yield Ok(Bytes::from(serde_json::to_string(&response).unwrap()));
            //             return;
            //         }
            //
            // }


            // Success response
            let response = ApiResponse::new(
                "success",
                "Successfully inserted records and merged staging.",
                StatusCode::OK,
                "".to_string(),
            );
            yield Ok(Bytes::from(serde_json::to_string(&response).unwrap()));

        };
        Box::pin(p_stream) // Explicitly pin the stream and return it
    }
}
