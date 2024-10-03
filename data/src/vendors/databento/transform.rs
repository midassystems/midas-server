use crate::error::Error;
use crate::error::Result;
use mbn::{self, records::Mbp1Msg};
use std::collections::HashMap;

pub fn instrument_id_map(
    dbn_map: HashMap<String, String>,
    mbn_map: HashMap<String, u32>,
) -> Result<HashMap<u32, u32>> {
    // Create the new map
    let mut map = HashMap::new();

    for (id, ticker) in dbn_map.iter() {
        if let Some(mbn_id) = mbn_map.get(ticker) {
            if let Ok(parsed_id) = id.parse::<u32>() {
                map.insert(parsed_id, *mbn_id);
            } else {
                return Err(Error::Conversion(format!("Failed to parse id: {}", id)));
            }
        }
    }
    Ok(map)
}

fn iterate_flag(block: &Vec<Mbp1Msg>, msg: &mut Mbp1Msg) -> Mbp1Msg {
    if block.iter().any(|m| m == msg) {
        // Duplicate found in the block
        msg.flags += 1;
        iterate_flag(block, msg)
    } else {
        msg.clone()
    }
}

pub fn to_mbn(
    records: Vec<databento::dbn::Mbp1Msg>,
    new_map: &HashMap<u32, u32>,
) -> Result<Vec<Mbp1Msg>> {
    let mut mbn_records = Vec::new();
    let mut rolling_block: Vec<Mbp1Msg> = Vec::new();

    for msg in records {
        let mut mbn_msg = Mbp1Msg::from(msg);

        if let Some(new_id) = new_map.get(&mbn_msg.hd.instrument_id) {
            mbn_msg.hd.instrument_id = *new_id;
        }

        if mbn_msg.flags == 0 {
            let updated_msg = iterate_flag(&rolling_block, &mut mbn_msg);
            rolling_block.push(updated_msg);
        } else {
            rolling_block.clear(); // Clear the rolling block
        }

        mbn_records.push(mbn_msg);
    }

    Ok(mbn_records)
}

pub fn find_duplicates(mbps: &Vec<mbn::records::Mbp1Msg>) -> Result<usize> {
    let mut occurrences = HashMap::new();
    let mut duplicates = Vec::new();

    for msg in mbps {
        // Only consider messages with non-zero flags as potential duplicates
        let count = occurrences.entry(msg.clone()).or_insert(0);
        *count += 1;
    }

    for msg in mbps {
        // Again, consider only messages with non-zero flags
        if let Some(&count) = occurrences.get(&msg) {
            if count > 1 {
                duplicates.push(msg);
            }
        }
    }

    println!("Duplicates : {:?}", duplicates);
    Ok(duplicates.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vendors::databento::client::databento_file_path;
    use crate::vendors::databento::extract::read_dbn_file;

    use crate::error::Result;
    use databento::dbn::{Dataset, Schema};
    use mbn::{
        self,
        records::{BidAskPair, RecordHeader},
    };
    use std::path::PathBuf;
    use time;
    fn setup(dir_path: &str) -> Result<PathBuf> {
        // Parameters
        let dataset = Dataset::GlbxMdp3;
        let start = time::macros::datetime!(2022-06-10 14:30 UTC);
        let end = time::macros::datetime!(2022-06-10 14:32 UTC);
        let schema = Schema::Mbp1;

        // Construct file path
        let file_path = databento_file_path(dir_path, &dataset, &schema, &start, &end)?;

        Ok(file_path)
    }

    #[tokio::test]
    async fn test_transform_to_mbn() -> Result<()> {
        // Load DBN file
        let file_path = setup("tests/data/databento").unwrap();
        let (records, map) = read_dbn_file(file_path).await?;

        // MBN instrument map
        let mut mbn_map = HashMap::new();
        mbn_map.insert("HE.n.0".to_string(), 20 as u32);

        // Map DBN instrument to MBN insturment
        let new_map = instrument_id_map(map, mbn_map)?;

        // Test
        let mbn_records = to_mbn(records, &new_map)?;

        // Validate
        assert!(mbn_records.len() > 0);

        Ok(())
    }

    #[test]
    fn test_to_mbn() {
        // Prepare test data
        let records = vec![
            Mbp1Msg {
                hd: RecordHeader {
                    length: 20,
                    rtype: 1,
                    instrument_id: 1333,
                    ts_event: 1724079906415347717,
                },
                price: 76025000000,
                size: 2,
                action: 84,
                side: 66,
                depth: 0,
                flags: 0,
                ts_recv: 1724079906416004321,
                ts_in_delta: 17171,
                sequence: 900097,
                levels: [BidAskPair {
                    bid_px: 76000000000,
                    ask_px: 76025000000,
                    bid_sz: 7,
                    ask_sz: 3,
                    bid_ct: 6,
                    ask_ct: 3,
                }],
            },
            Mbp1Msg {
                hd: RecordHeader {
                    length: 20,
                    rtype: 1,
                    instrument_id: 1333,
                    ts_event: 1724079906415347717,
                },
                price: 76025000000,
                size: 1,
                action: 84,
                side: 66,
                depth: 0,
                flags: 0,
                ts_recv: 1724079906416018707,
                ts_in_delta: 13985,
                sequence: 900098,
                levels: [BidAskPair {
                    bid_px: 76000000000,
                    ask_px: 76025000000,
                    bid_sz: 7,
                    ask_sz: 3,
                    bid_ct: 6,
                    ask_ct: 3,
                }],
            },
            Mbp1Msg {
                hd: RecordHeader {
                    length: 20,
                    rtype: 1,
                    instrument_id: 1333,
                    ts_event: 1724079906415347717,
                },
                price: 76050000000,
                size: 1,
                action: 84,
                side: 66,
                depth: 0,
                flags: 0,
                ts_recv: 1724079906416018707,
                ts_in_delta: 13985,
                sequence: 900098,
                levels: [BidAskPair {
                    bid_px: 76000000000,
                    ask_px: 76025000000,
                    bid_sz: 7,
                    ask_sz: 3,
                    bid_ct: 6,
                    ask_ct: 3,
                }],
            },
            Mbp1Msg {
                hd: RecordHeader {
                    length: 20,
                    rtype: 1,
                    instrument_id: 1333,
                    ts_event: 1724079906415347717,
                },
                price: 76050000000,
                size: 5,
                action: 84,
                side: 66,
                depth: 0,
                flags: 0,
                ts_recv: 1724079906416018707,
                ts_in_delta: 13985,
                sequence: 900098,
                levels: [BidAskPair {
                    bid_px: 76000000000,
                    ask_px: 76025000000,
                    bid_sz: 7,
                    ask_sz: 3,
                    bid_ct: 6,
                    ask_ct: 3,
                }],
            },
            Mbp1Msg {
                hd: RecordHeader {
                    length: 20,
                    rtype: 1,
                    instrument_id: 1333,
                    ts_event: 1724079906415347717,
                },
                price: 76050000000,
                size: 1,
                action: 84,
                side: 66,
                depth: 0,
                flags: 0,
                ts_recv: 1724079906416018707,
                ts_in_delta: 13985,
                sequence: 900098,
                levels: [BidAskPair {
                    bid_px: 76000000000,
                    ask_px: 76025000000,
                    bid_sz: 7,
                    ask_sz: 3,
                    bid_ct: 6,
                    ask_ct: 3,
                }],
            },
            Mbp1Msg {
                hd: RecordHeader {
                    length: 20,
                    rtype: 1,
                    instrument_id: 1333,
                    ts_event: 1724079906415347717,
                },
                price: 76075000000,
                size: 1,
                action: 84,
                side: 66,
                depth: 0,
                flags: 0,
                ts_recv: 1724079906416027586,
                ts_in_delta: 13247,
                sequence: 900099,
                levels: [BidAskPair {
                    bid_px: 76000000000,
                    ask_px: 76025000000,
                    bid_sz: 7,
                    ask_sz: 3,
                    bid_ct: 6,
                    ask_ct: 3,
                }],
            },
            Mbp1Msg {
                hd: RecordHeader {
                    length: 20,
                    rtype: 1,
                    instrument_id: 1333,
                    ts_event: 1724079906415347717,
                },
                price: 76075000000,
                size: 3,
                action: 84,
                side: 66,
                depth: 0,
                flags: 0,
                ts_recv: 1724079906416027586,
                ts_in_delta: 13247,
                sequence: 900099,
                levels: [BidAskPair {
                    bid_px: 76000000000,
                    ask_px: 76025000000,
                    bid_sz: 7,
                    ask_sz: 3,
                    bid_ct: 6,
                    ask_ct: 3,
                }],
            },
            Mbp1Msg {
                hd: RecordHeader {
                    length: 20,
                    rtype: 1,
                    instrument_id: 1333,
                    ts_event: 1724079906415347717,
                },
                price: 76025000000,
                size: 2,
                action: 84,
                side: 66,
                depth: 0,
                flags: 0,
                ts_recv: 1724079906416004321,
                ts_in_delta: 17171,
                sequence: 900097,
                levels: [BidAskPair {
                    bid_px: 76000000000,
                    ask_px: 76025000000,
                    bid_sz: 7,
                    ask_sz: 3,
                    bid_ct: 6,
                    ask_ct: 3,
                }],
            },
            Mbp1Msg {
                hd: RecordHeader {
                    length: 20,
                    rtype: 1,
                    instrument_id: 1333,
                    ts_event: 1724079906415347717,
                },
                price: 76075000000,
                size: 1,
                action: 67,
                side: 65,
                depth: 0,
                flags: 128,
                ts_recv: 1724079906416365178,
                ts_in_delta: 20366,
                sequence: 900100,
                levels: [BidAskPair {
                    bid_px: 76000000000,
                    ask_px: 76100000000,
                    bid_sz: 7,
                    ask_sz: 10,
                    bid_ct: 6,
                    ask_ct: 5,
                }],
            },
        ];

        // Test
        let mut mbn_records = Vec::new();
        let mut rolling_block: Vec<Mbp1Msg> = Vec::new(); // To track the last message for comparison
                                                          // let mut flag_counter = 0;

        // for mut msg in records {
        //     println!("Current Message: {:?}", msg);

        //     if msg.flags == 0 {
        //         // Check for duplicates in the rolling block
        //         if rolling_block.iter().any(|m| *m == msg) {
        //             // Duplicate found in the block
        //             flag_counter += 1;
        //             msg.flags = flag_counter;
        //         } else {
        //             // No duplicate, reset counter
        //             flag_counter = 0;
        //             rolling_block.push(msg.clone()); // Add to the rolling block
        //         }
        //     } else {
        //         // Non-zero flag encountered, reset the block
        //         flag_counter = 0;
        //         rolling_block.clear(); // Clear the rolling block
        //     }

        //     println!("Flag counter: {:?}", flag_counter);
        //     println!("Updated Message: {:?}", msg);

        //     mbn_records.push(msg);
        // }

        fn iterate_flag(block: &Vec<Mbp1Msg>, msg: &mut Mbp1Msg) -> Mbp1Msg {
            if block.iter().any(|m| m == msg) {
                // Duplicate found in the block
                msg.flags += 1;
                iterate_flag(block, msg)
                // flag_counter +=/*   */1;
            } else {
                msg.clone()
                // // No duplicate, reset counter
                // // flag_counter = 0;
                // rolling_block.push(msg.clone()); // Add to the rolling block
            }
        }

        for mut msg in records {
            println!("Current Message: {:?}", msg);

            if msg.flags == 0 {
                let updated_msg = iterate_flag(&rolling_block, &mut msg);
                rolling_block.push(updated_msg);
                // Check for duplicates in the rolling block
                // if rolling_block.iter().any(|m| *m == msg) {
                //     // Duplicate found in the block
                //     msg.flags += 1;
                //     // flag_counter +=/*   */1;
                // } else {
                //     // No duplicate, reset counter
                //     // flag_counter = 0;
                //     rolling_block.push(msg.clone()); // Add to the rolling block
                // }
            } else {
                // Non-zero flag encountered, reset the block
                // flag_counter = 0;
                rolling_block.clear(); // Clear the rolling block
            }

            // println!("Flag counter: {:?}", flag_counter);
            println!("Updated Message: {:?}", msg);

            mbn_records.push(msg);
        }

        println!("Final Records with Flags: {:?}", mbn_records);
        let _ = find_duplicates(&mbn_records);
    }
}
