use crate::Result;
use std::fs::File;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry;

pub fn system_logger(log_file: String, log_level: String) -> Result<()> {
    // Create a file appender
    let file = File::create(log_file)?;

    // Create a file layer with JSON formatting
    let file_layer = Layer::new()
        .json()
        .with_writer(file)
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(true)
        .with_span_events(FmtSpan::CLOSE); // Adding span close events for better traceability

    // Create an EnvFilter layer to control log levels
    let filter_layer = EnvFilter::new(log_level);

    // Create a subscriber with the file layer and the filter layer
    let subscriber = Registry::default().with(file_layer).with(filter_layer);

    // Set the subscriber as the global default
    tracing::subscriber::set_global_default(subscriber)?;

    tracing::info!("Logging system initialized successfully.");

    Ok(())
}
