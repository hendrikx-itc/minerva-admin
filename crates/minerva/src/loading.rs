use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::io::Write;

use chrono::DateTime;
use tokio_postgres::{GenericClient, Client};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use futures_util::SinkExt;

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
) -> Result<(), String> {
    println!("Loading file {}", file_path.as_ref().to_string_lossy());

    let description = format!("{{\"csv-load\": \"{}\"}}", file_path.as_ref().to_string_lossy());

    let f = File::open(file_path).map_err(|e| format!("{}", e))?;

    let reader = BufReader::new(f);

    let mut csv_reader = csv::Reader::from_reader(reader);

    let job_id = start_job(client, &description).await?;

<<<<<<< HEAD
     let data_package: Vec<(i64, DateTime<chrono::Utc>, Vec<String>)> = csv_reader.records().map(|record| {
        let record = record.unwrap();
=======
    for result in csv_reader.records() {
        let record = result.unwrap();
>>>>>>> 7e425f3 (Initial data loading logic)

        println!("{:?}", record);

        let entity: &str = record.get(0).unwrap();
        let timestamp_txt: &str = record.get(1).unwrap();

        let timestamp: DateTime<chrono::Utc> = DateTime::parse_from_rfc3339(timestamp_txt).unwrap().with_timezone(&chrono::offset::Utc);

        println!("Entity: {}", entity);
        println!("Timestamp: {}", timestamp);

        let values: Vec<String> = Vec::new();

        let record: (i64, DateTime<chrono::Utc>, Vec<String>) = (100, timestamp, values);

        record
    }).collect();

    let trend_store_part = "datasource_entitytype_granularity";
    let timestamp = chrono::offset::Utc::now();

    store_copy_from(client, job_id, data_package).await?;
    mark_modfied(client, trend_store_part, timestamp).await?;

    println!("Job ID: {job_id}");

    end_job(client, job_id).await?;

    println!("Job ID: {job_id}");

    end_job(client, job_id).await?;

    Ok(())
}

async fn start_job<T: GenericClient + Send + Sync>(client: &mut T, description: &str) -> Result<i64, String> {
    let query = "SELECT logging.start_job($1)";

    let result = client
        .query_one(query, &[&description])
        .await
        .map_err(|e| format!("Error starting job: {e}"))?;

    let job_id = result.get(0);

    Ok(job_id)
}

async fn end_job<T: GenericClient + Send + Sync>(client: &mut T, job_id: i64) -> Result<(), String> {
    let query = "SELECT logging.end_job($1)";

    client
        .execute(query, &[&job_id])
        .await
        .map_err(|e| format!("Error ending job: {e}"))?;

    Ok(())
}

async fn start_job<T: GenericClient + Send + Sync>(client: &mut T, description: &str) -> Result<i64, String> {
    let query = "SELECT logging.start_job($1::text::jsonb)";

    let result = client
        .query_one(query, &[&description])
        .await
        .map_err(|e| format!("Error starting job: {e}"))?;

    let job_id = result.get(0);

    Ok(job_id)
}

async fn end_job<T: GenericClient + Send + Sync>(client: &mut T, job_id: i64) -> Result<(), String> {
    let query = "SELECT logging.end_job($1)";

    client
        .execute(query, &[&job_id])
        .await
        .map_err(|e| format!("Error ending job: {e}"))?;

    Ok(())
}

async fn store_copy_from(client: &mut Client, job_id: i64, data_package: Vec<(i64, DateTime<chrono::Utc>, Vec<String>)>) -> Result<(), String> {
    let buf: Vec<u8> = Vec::new();
    let tx = client.transaction().await.unwrap();
    
    let query = format!("COPY ");

    let copy_in_sink = tx.copy_in::<String, &[u8]>(&query).await.unwrap();

    //let bytes = buf.try_into().unwrap();

    let bytes: [u8; 100] = [0u8;100];

    copy_in_sink.write_all(&bytes);

    tx.commit();

    Ok(())
}

async fn mark_modfied<T: GenericClient + Send + Sync>(client: &mut T, trend_store_part: &str, timestamp: DateTime<chrono::Utc>) -> Result<(), String> {
    Ok(())
}
