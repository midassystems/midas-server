use crate::error::Result;
use midas_client::historical::Historical;
use std::path::PathBuf;

/// Main function for loading data to database
pub async fn load_file_to_db(file_name: &PathBuf, client: &Historical) -> Result<()> {
    // Convert PathBuf to String
    let path_string: String = file_name.to_string_lossy().into_owned();

    let _ = client.create_mbp_from_file(&path_string).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::databento::{
        client::databento_file_path,
        extract::read_dbn_file,
        transform::{instrument_id_map, mbn_to_file, to_mbn},
    };
    use crate::error::Result;
    use crate::tickers::get_tickers;
    use databento::dbn::{Dataset, Schema};
    use serial_test::serial;
    use std::path::PathBuf;
    use time;

    fn setup(dir_path: &PathBuf) -> Result<PathBuf> {
        // Parameters
        let dataset = Dataset::GlbxMdp3;
        let start = time::macros::datetime!(2024-08-20 00:00 UTC);
        let end = time::macros::datetime!(2024-08-20 05:00 UTC);
        let schema = Schema::Mbp1;

        // Construct file path
        let file_path = databento_file_path(dir_path, &dataset, &schema, &start, &end)?;

        Ok(file_path)
    }

    #[tokio::test]
    #[serial]
    // #[ignore]
    async fn test_load_file_to_db() -> Result<()> {
        let base_url = "http://localhost:8080"; // Update with your actual base URL
        let client = Historical::new(base_url);

        // Create Instruments
        let (mbn_map, _grouped_tickers) =
            get_tickers("tests/tickers.json", "databento", &client).await?;

        // Load DBN file
        let file_path = setup(&PathBuf::from("tests/data/databento"))?;
        // let file_path = setup("tests/data/databento").unwrap();
        let (mut records, map) = read_dbn_file(file_path).await?;

        // Create the new map
        let new_map = instrument_id_map(map, mbn_map.clone())?;

        // Convert Records oto MBN
        let mbn_records = to_mbn(&mut records, &new_map).await?; // Now you have new_map = {id: mbn_id}

        // -- To MBN file
        let mbn_file_name = PathBuf::from("../data/load_testing_file.bin");
        let _ = mbn_to_file(&mbn_records, &mbn_file_name).await?;

        // Test
        let path = PathBuf::from("data/load_testing_file.bin");
        let _ = load_file_to_db(&path, &client).await?;

        // Cleanup
        for value in mbn_map.values() {
            let _ = client.delete_symbol(&(*value as i32)).await?;
        }

        Ok(())
    }
}

// /// Doesnt pass test may delete and strictly load from file
// pub async fn load_stream_to_db(records: &Vec<Mbp1Msg>, client: &ApiClient) -> Result<()> {
//     // Create RecordRef vector.
//     let mut refs = Vec::new();
//     for msg in records {
//         refs.push(RecordRef::from(msg));
//     }
//
//     // Enocde records.
//     let mut buffer = Vec::new();
//     let mut encoder = RecordEncoder::new(&mut buffer);
//     encoder.encode_records(&refs).expect("Encoding failed");
//
//     // Add to database
//     let response = client.create_mbp(&buffer).await?;
//     println!("{:?}", response.message);
//
//     Ok(())
// }
// #[tokio::test]
// #[serial]
// #[ignore]
// async fn test_load_stream_to_db() -> Result<()> {
//     let base_url = "http://localhost:8080"; // Update with your actual base URL
//     let client = ApiClient::new(base_url);
//
//     // Create Instruments
//     let (mbn_map, _grouped_tickers) =
//         get_tickers("tests/tickers.json", "databento", &client).await?;
//
//     // Load DBN file
//     let file_path = setup(&PathBuf::from("tests/data"))?;
//     let (records, map) = read_dbn_file(file_path).await?;
//
//     // Create the new map
//     let new_map = instrument_id_map(map, mbn_map.clone())?;
//
//     // Convert Records oto MBN
//     let mbn_records = to_mbn(records, &new_map)?;
//     println!("{:?}", mbn_records);
//
//     // -- To MBN file
//     // let mbn_file_name = format!("../data/load_testing_file.bin",);
//     // let _ = mbn_to_file(&mbn_records, &mbn_file_name).await?;
//
//     // Test
//     let _ = load_stream_to_db(&mbn_records, &client).await?;
//     // let _ = load_file_to_db("data/load_testing_file.bin", &client).await?;
//
//     // Cleanup
//     for value in mbn_map.values() {
//         let _ = client.delete_symbol(&(*value as i32)).await?;
//     }
//
//     Ok(())
// }
