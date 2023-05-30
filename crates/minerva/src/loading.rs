use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use async_trait::async_trait;
use chrono::DateTime;
use postgres_types::{ToSql, Type};
use tokio_postgres::{GenericClient, Client, binary_copy::BinaryCopyInWriter};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;

use futures_util::pin_mut;

use crate::interval::parse_interval;
use crate::trend_store::get_trend_store_id;
use crate::trend_store::{MeasurementStore, TrendStore, TrendStorePart, load_trend_store, create_partitions_for_trend_store_and_timestamp};
use crate::job::{start_job, end_job};

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
) -> Result<(), String> {
    println!("Loading file {}", file_path.as_ref().to_string_lossy());

    let description = json!({"csv-load": file_path.as_ref().to_string_lossy()});

    let f = File::open(file_path).map_err(|e| format!("{}", e))?;

    let reader = BufReader::new(f);

    let mut csv_reader = csv::Reader::from_reader(reader);

    let job_id = start_job(client, &description).await?;

    let data_package: Vec<(i64, DateTime<chrono::Utc>, Vec<String>)> = csv_reader.records().enumerate().map(|(num, record)| {
        let record = record.unwrap();

        println!("{:?}", record);

        let entity: &str = record.get(0).unwrap();
        let timestamp_txt: &str = record.get(1).unwrap();

        let timestamp: DateTime<chrono::Utc> = DateTime::parse_from_rfc3339(timestamp_txt).unwrap().with_timezone(&chrono::offset::Utc);

        println!("Entity: {}", entity);
        println!("Timestamp: {}", timestamp);

        let mut values: Vec<String> = Vec::new();

        for i in 0..record.len() - 2 {
            values.push(record.get(i + 2).unwrap().to_string());
        }

        let record: (i64, DateTime<chrono::Utc>, Vec<String>) = (num as i64, timestamp, values);

        record
    }).collect();

    let timestamp = chrono::offset::Utc::now();

    let granularity = parse_interval(&parser_config.granularity).unwrap();

    let trend_store: TrendStore = load_trend_store(client, data_source, &parser_config.entity_type, &granularity)
        .await
        .map_err(|e| format!("Error loading trend store for data source '{data_source}', entity type '{}' and granularity '{}': {e}", parser_config.entity_type, parser_config.granularity))?;

    let trend_store_id: i32 = get_trend_store_id(client, &trend_store).await.map_err(|e| format!("Error loading trend store Id from database: {e}"))?;

    if create_partitions {
        for record in &data_package  {
            create_partitions_for_trend_store_and_timestamp(client, trend_store_id,  record.1).await.map_err(|e| format!("Error creating partition for timestamp: {e}"))?;
        }
    }

    let trend_store_part: TrendStorePart = trend_store.parts.first().unwrap().clone();

    trend_store_part.store_copy_from(client, job_id, data_package).await?;

    trend_store_part.mark_modified(client, timestamp).await?;

    println!("Job ID: {job_id}");

    end_job(client, job_id).await?;

    Ok(())
}


#[derive(Debug)]
pub enum MeasValue {
    Integer(i32),
    Int8(i64),
    Real(f64),
    Text(String),
    Timestamp(chrono::DateTime<chrono::Utc>),
    Numeric(Decimal),
}

trait ToType {
    fn to_type(&self) -> &Type;
}

impl ToType for MeasValue {
    fn to_type(&self) -> &Type {
        match self {
            MeasValue::Integer(_) => &Type::INT4,
            MeasValue::Int8(_) => &Type::INT8,
            MeasValue::Real(_) => &Type::NUMERIC,
            MeasValue::Text(_) => &Type::TEXT,
            MeasValue::Timestamp(_) => &Type::TIMESTAMPTZ,
            MeasValue::Numeric(_) => &Type::NUMERIC,
        }
    }
}

impl ToSql for MeasValue {
    fn to_sql(&self, ty: &Type, out: &mut bytes::BytesMut) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>>
        where
            Self: Sized {
        match self {
            MeasValue::Integer(x) => x.to_sql(ty, out),
            MeasValue::Int8(x) => x.to_sql(ty, out),
            MeasValue::Real(x) => x.to_sql(ty, out),
            MeasValue::Text(x) => x.to_sql(ty, out),
            MeasValue::Timestamp(x) => x.to_sql(ty, out),
            MeasValue::Numeric(x) => x.to_sql(ty, out),
        }
    }

    fn accepts(_ty: &Type) -> bool
        where
            Self: Sized {
        true
    }

    fn to_sql_checked(
            &self,
            ty: &Type,
            out: &mut bytes::BytesMut,
        ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            MeasValue::Integer(x) => x.to_sql_checked(ty, out),
            MeasValue::Int8(x) => x.to_sql_checked(ty, out),
            MeasValue::Real(x) => x.to_sql_checked(ty, out),
            MeasValue::Text(x) => x.to_sql_checked(ty, out),
            MeasValue::Timestamp(x) => x.to_sql_checked(ty, out),
            MeasValue::Numeric(x) => x.to_sql_checked(ty, out),
        }
    }
}

fn copy_from_query(trend_store_part: &TrendStorePart) -> String {
    let trend_names_part = trend_store_part.trends
        .iter()
        .map(|t| format!("\"{}\"", t.name))
        .collect::<Vec<_>>()
        .join(", ");

    let query = format!(
        "COPY trend.{}(entity_id, timestamp, created, job_id, {}) FROM STDIN BINARY", trend_store_part.name, &trend_names_part
    );

    query
}

#[async_trait]
impl MeasurementStore for TrendStorePart {
    async fn store_copy_from(&self, client: &mut Client, job_id: i64, data_package: Vec<(i64, DateTime<chrono::Utc>, Vec<String>)>) -> Result<(), String> {
        let tx = client.transaction().await.unwrap();
        
        let query = copy_from_query(self);
    
        let copy_in_sink = tx.copy_in(&query).await.map_err(|e| format!("Error starting COPY command: {}", e))?;
    
        let value_types: Vec<Type> = vec![
            Type::INT4,
            Type::TIMESTAMPTZ,
            Type::TIMESTAMPTZ,
            Type::INT8,
            Type::NUMERIC,
            Type::NUMERIC,
            Type::NUMERIC,
            Type::NUMERIC
        ];
    
        let binary_copy_writer = BinaryCopyInWriter::new(copy_in_sink, &value_types);
        pin_mut!(binary_copy_writer);
    
        for (entity_id, timestamp, vals) in data_package {
            let mut measurements: Vec<MeasValue> = Vec::new();
            measurements.push(MeasValue::Integer(entity_id as i32));
            measurements.push(MeasValue::Timestamp(timestamp));
            measurements.push(MeasValue::Timestamp(timestamp));
            measurements.push(MeasValue::Int8(job_id));

            for val in vals {
                measurements.push(MeasValue::Numeric(Decimal::from_str(&val).unwrap()));
            }
        
            let values: Vec<&'_ (dyn ToSql + Sync)> = measurements.iter().map(|v| v as &(dyn ToSql + Sync)).collect();
        
            binary_copy_writer.as_mut().write(&values).await.map_err(|e| format!("Error writing row: {e}"))?
        }
    
        binary_copy_writer.finish().await.unwrap();
    
        tx.commit().await.unwrap();
    
        Ok(())
    }

    async fn mark_modified<T: GenericClient + Send + Sync>(&self, client: &mut T, timestamp: DateTime<chrono::Utc>) -> Result<(), String> {
        let query = "SELECT trend_directory.mark_modified(id, $2) FROM trend_directory.trend_store_part WHERE name = $1";

        client
            .execute(query, &[&self.name, &timestamp])
            .await
            .map_err(|e| format!("Error marking timestamp as modified: {e}"))?;

        Ok(())
    }   
}
