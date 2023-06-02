use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use chrono::DateTime;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio_postgres::Client;

use crate::error::Error;
use crate::interval::parse_interval;
use crate::job::{end_job, start_job};
use crate::trend_store::get_trend_store_id;
use crate::trend_store::{
    create_partitions_for_trend_store_and_timestamp, load_trend_store, MeasurementStore,
    TrendStore, TrendStorePart,
};

#[derive(Serialize, Deserialize)]
pub struct ParserConfig {
    pub entity_type: String,
    pub granularity: String,
    pub extra: Option<Value>,
}

pub async fn load_data<P: AsRef<Path>>(
    client: &mut Client,
    data_source: &str,
    parser_config: &ParserConfig,
    file_path: P,
    create_partitions: bool,
) -> Result<(), Error> {
    println!("Loading file {}", file_path.as_ref().to_string_lossy());

    let description = json!({"csv-load": file_path.as_ref().to_string_lossy()});

    let f = File::open(file_path).map_err(|e| format!("{}", e))?;

    let reader = BufReader::new(f);

    let mut csv_reader = csv::Reader::from_reader(reader);

    let job_id = start_job(client, &description).await?;

    let data_package: Vec<(i64, DateTime<chrono::Utc>, Vec<String>)> = csv_reader
        .records()
        .enumerate()
        .map(|(num, record)| {
            let record = record.unwrap();

            println!("{:?}", record);

            let entity: &str = record.get(0).unwrap();
            let timestamp_txt: &str = record.get(1).unwrap();

            let timestamp: DateTime<chrono::Utc> = DateTime::parse_from_rfc3339(timestamp_txt)
                .unwrap()
                .with_timezone(&chrono::offset::Utc);

            println!("Entity: {}", entity);
            println!("Timestamp: {}", timestamp);

            let mut values: Vec<String> = Vec::new();

            for i in 0..record.len() - 2 {
                values.push(record.get(i + 2).unwrap().to_string());
            }

            let record: (i64, DateTime<chrono::Utc>, Vec<String>) = (num as i64, timestamp, values);

            record
        })
        .collect();

    let timestamp = chrono::offset::Utc::now();

    let granularity = parse_interval(&parser_config.granularity).unwrap();

    let trend_store: TrendStore = load_trend_store(client, data_source, &parser_config.entity_type, &granularity)
        .await
        .map_err(|e| format!("Error loading trend store for data source '{data_source}', entity type '{}' and granularity '{}': {e}", parser_config.entity_type, parser_config.granularity))?;

    let trend_store_id: i32 = get_trend_store_id(client, &trend_store)
        .await
        .map_err(|e| format!("Error loading trend store Id from database: {e}"))?;

    if create_partitions {
        for record in &data_package {
            create_partitions_for_trend_store_and_timestamp(client, trend_store_id, record.1)
                .await
                .map_err(|e| format!("Error creating partition for timestamp: {e}"))?;
        }
    }

    let trend_store_part: TrendStorePart = trend_store.parts.first().unwrap().clone();

    let trends: Vec<String> = Vec::new();

    trend_store_part
        .store_copy_from(client, job_id, &trends, data_package)
        .await?;

    trend_store_part.mark_modified(client, timestamp).await?;

    println!("Job ID: {job_id}");

    end_job(client, job_id).await?;

    Ok(())
}
