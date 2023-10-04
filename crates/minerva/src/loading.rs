use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::collections::HashMap;
use std::iter::zip;

use chrono::DateTime;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio_postgres::Client;

use crate::error::{Error, RuntimeError};
use crate::interval::parse_interval;
use crate::job::{end_job, start_job};
use crate::trend_store::get_trend_store_id;
use crate::trend_store::{
    create_partitions_for_trend_store_and_timestamp, load_trend_store, MeasurementStore,
    TrendStore, TrendStorePart
};

#[derive(Serialize, Deserialize)]
pub struct TrendsFromHeader {
    pub entity_column: String,
    pub timestamp_column: String,
}

#[derive(Serialize, Deserialize)]
pub enum TrendsFrom {
    List(Vec<String>),
    Header(TrendsFromHeader),
}

#[derive(Serialize, Deserialize)]
pub struct ParserConfig {
    pub entity_type: String,
    pub granularity: String,
    pub trends: TrendsFrom,
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

    let (trends, entity_column, timestamp_column) = match &parser_config.trends {
        TrendsFrom::Header(from_header) => {
            let trends = match csv_reader.headers() {
                Ok(headers) => headers.iter().map(String::from).collect(),
                Err(_) => Vec::new()
            };

            (trends, from_header.entity_column.clone(), from_header.timestamp_column.clone())
        },
        TrendsFrom::List(list) => (list.clone(), String::from("entity"), String::from("timestamp"))
    };

    let entity_column_index = match trends.iter().position(|t| t.eq(&entity_column)) {
        Some(index) => index,
        None => return Err(Error::Runtime(RuntimeError::from_msg(format!("No column matching entity column '{entity_column}'")))),
    };

    let timestamp_column_index = match trends.iter().position(|t| t.eq(&timestamp_column)) {
        Some(index) => index,
        None => return Err(Error::Runtime(RuntimeError::from_msg(format!("No column matching timestamp column '{timestamp_column}'")))),
    };

    let job_id = start_job(client, &description).await?;

    let raw_data_package: Vec<(String, DateTime<chrono::Utc>, Vec<String>)> = csv_reader
        .records()
        .map(|record| {
            let record = record.unwrap();

            let entity: String = String::from(record.get(entity_column_index).unwrap());
            let timestamp_txt: &str = record.get(timestamp_column_index).unwrap();

            let timestamp: DateTime<chrono::Utc> = DateTime::parse_from_rfc3339(timestamp_txt)
                .unwrap()
                .with_timezone(&chrono::offset::Utc);

            let values: Vec<String> = record.iter().map(String::from).collect();

            let record: (String, DateTime<chrono::Utc>, Vec<String>) = (entity, timestamp, values);

            record
        })
        .collect();

    let entity_names: Vec<String> = raw_data_package.iter().map(|r| r.0.clone()).collect();

    let timestamp = chrono::offset::Utc::now();

    let granularity = parse_interval(&parser_config.granularity).unwrap();

    let trend_store: TrendStore = load_trend_store(client, data_source, &parser_config.entity_type, &granularity)
        .await
        .map_err(|e| format!("Error loading trend store for data source '{data_source}', entity type '{}' and granularity '{}': {e}", parser_config.entity_type, parser_config.granularity))?;

    let entity_ids: Vec<i32> = names_to_entity_ids(
        client,
        &trend_store.entity_type,
        entity_names,
    )
    .await?;

    let data_package: Vec<(i32, DateTime<chrono::Utc>, Vec<String>)> = zip(entity_ids, raw_data_package)
        .into_iter()
        .map(|(entity_id, record)| {
            (entity_id, record.1, record.2)
        })
        .collect();

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

    trend_store_part
        .store_raw(client, job_id, &trends, &data_package)
        .await?;

    trend_store_part.mark_modified(client, timestamp).await?;

    println!("Job ID: {job_id}");

    end_job(client, job_id).await?;

    Ok(())
}

async fn names_to_entity_ids<I>(
    client: &mut Client,
    entity_type: &str,
    names: I,
) -> Result<Vec<i32>, Error>
where
    I: IntoIterator,
    I::Item: ToString,
{
    let row = client
        .query_one(
            "SELECT name \
            FROM directory.entity_type \
            WHERE lower(name) = lower($1)",
            &[&entity_type],
        )
        .await?;

    let entity_type_name: String = row.get(0);

    let query = format!(
        "WITH lookup_list AS (SELECT unnest($1::text[]) AS name) \
        SELECT l.name, e.id FROM lookup_list l \
        LEFT JOIN entity.\"{}\" e ON l.name = e.name ",
        &entity_type_name
    );

    let names_list: Vec<String> = names.into_iter().map(|n| n.to_string()).collect();

    let rows = client.query(&query, &[&names_list]).await?;

    let mut entity_ids: HashMap<String, i32> = HashMap::new();

    for row in rows {
        let name: String = row.get(0);
        let entity_id_value: Option<i32> = row.try_get(1).unwrap();
        let entity_id: i32 = match entity_id_value {
            Some(entity_id) => entity_id,
            None => create_entity(client, entity_type, &name).await?,
        };

        entity_ids.insert(name, entity_id);
    }

    Ok(names_list.iter().map(|name| *(entity_ids.get(name).unwrap())).collect())
}

async fn create_entity(client: &mut Client, entity_type: &str, name: &str) -> Result<i32, Error> {
    let row = client
        .query_one(
            "SELECT name FROM directory.entity_type WHERE lower(name) = lower($1)",
            &[&entity_type],
        )
        .await?;

    let entity_type_name: String = row.get(0);

    let query = format!(
        "INSERT INTO entity.\"{}\"(name) VALUES($1) ON CONFLICT(name) DO NOTHING RETURNING id",
        &entity_type_name
    );

    let rows = client.query(&query, &[&name]).await?;

    match rows.iter().next() {
        Some(row) => row.try_get(0).map_err(|e| {
            Error::Runtime(RuntimeError::from_msg(format!(
                "Could not create entity: {e}"
            )))
        }),
        None => Err(Error::Runtime(RuntimeError::from_msg(format!(
            "Could not insert entity"
        )))),
    }
}
