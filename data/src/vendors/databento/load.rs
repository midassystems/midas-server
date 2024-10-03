use crate::error::Result;
use mbn::{self, encode::RecordEncoder, record_ref::RecordRef, records::Mbp1Msg};
use midas_client::client::ApiClient;

pub async fn load_to_db(records: &Vec<Mbp1Msg>, client: &ApiClient) -> Result<()> {
    // Create RecordRef vector.
    let mut refs = Vec::new();
    for msg in records {
        refs.push(RecordRef::from(msg));
    }

    // Enocde records.
    let mut buffer = Vec::new();
    let mut encoder = RecordEncoder::new(&mut buffer);
    encoder.encode_records(&refs).expect("Encoding failed");

    // Add to database
    let response = client.create_mbp(&buffer).await?;
    println!("{:?}", response.message);

    Ok(())
}

pub async fn load_file_to_db(file_name: &str, client: &ApiClient) -> Result<()> {
    let response = client.create_mbp_from_file(file_name).await?;
    println!("{:?}", response);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::vendors::databento::client::databento_file_path;
    use crate::vendors::databento::extract::read_dbn_file;
    use crate::vendors::databento::transform::{instrument_id_map, to_mbn};
    use databento::dbn::{Dataset, Schema};
    use mbn::symbols::Instrument;
    use std::{collections::HashMap, path::PathBuf};
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
    async fn test_read_dbn_stream_file() -> Result<()> {
        let base_url = "http://localhost:8080"; // Update with your actual base URL
        let client = ApiClient::new(base_url);

        // Create Instruments
        let instrument1 = Instrument {
            ticker: "HE.n.0".to_string(),
            name: "Apple tester client".to_string(),
            instrument_id: None,
        };

        let create_response = client.create_symbol(&instrument1).await?;
        let id_1 = create_response.data.unwrap();

        let instrument2 = Instrument {
            ticker: "ZC.n.0".to_string(),
            name: "tester client".to_string(),
            instrument_id: None,
        };
        let create_response = client.create_symbol(&instrument2).await?;
        let id_2 = create_response.data.unwrap();

        // Mbn instrument map
        let mut mbn_map = HashMap::new();
        mbn_map.insert(instrument1.ticker, id_1 as u32);
        mbn_map.insert(instrument2.ticker, id_2 as u32);

        // Load DBN file
        let file_path = setup("tests/data/databento").unwrap();
        let (records, map) = read_dbn_file(file_path).await?;

        // Create the new map
        let new_map = instrument_id_map(map, mbn_map)?;

        // Convert Records oto MBN
        let mbn_records = to_mbn(records, &new_map)?; // Now you have new_map = {id: mbn_id}

        // Test
        let _ = load_to_db(&mbn_records, &client).await?;

        // Cleanup
        let _ = client.delete_symbol(&(id_1 as i32)).await?;
        let _ = client.delete_symbol(&(id_2 as i32)).await?;

        Ok(())
    }
}
