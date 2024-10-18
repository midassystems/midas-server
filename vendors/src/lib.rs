pub mod databento;
pub mod error;
pub mod tickers;
pub mod utils;
pub use error::{Error, Result};

// use once_cell::sync::Lazy;
//
// // pub static RAW_DIR: Lazy<String> =
//     Lazy::new(|| std::env::var("RAW_DIR").expect("Raw data directory not set"));
//
// pub static PROCESSED_DIR: Lazy<String> =
//     Lazy::new(|| std::env::var("PROCESSED_DIR").expect("Raw data directory not set"));
//
// pub static DATA_DIR: Lazy<String> = Lazy::new(|| {
//     match std::env::var("MODE") {
//         Ok(_) => {
//             println!("testing");
//             String::from("tests/data") // Use "tests/data" for tests
//         }
//         Err(_) => {
//             println!("production");
//             String::from("data") // Use "data" for normal execution
//         }
//     }
// });
//
// pub static MBN_DATA_DIR: Lazy<String> = Lazy::new(|| {
//     match std::env::var("MODE") {
//         Ok(_) => {
//             println!("testing");
//             String::from("../data") // Use "tests/data" for tests
//         }
//         Err(_) => {
//             println!("production");
//             String::from("data") // Use "data" for normal execution
//         }
//     }
// });
