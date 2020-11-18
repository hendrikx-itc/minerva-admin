use postgres::Client;
use postgres::types::ToSql;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use serde_json::{Value, json};

type PostgresName = String;


#[derive(Debug, Serialize, Deserialize, Clone, ToSql)]
#[postgres(name = "trend_descr")]
pub struct Trend {
    pub name: PostgresName,
    pub data_type: String,
    #[serde(default="default_empty_string")]
    pub description: String,
    #[serde(default="default_time_aggregation")]
    pub time_aggregation: String,
    #[serde(default="default_entity_aggregation")]
    pub entity_aggregation: String,
    #[serde(default="default_extra_data")]
    pub extra_data: Value,
}

fn default_time_aggregation() -> String {
    String::from("SUM")
}

fn default_entity_aggregation() -> String {
    String::from("SUM")
}

fn default_extra_data() -> Value {
    json!("{}")
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSql)]
#[postgres(name = "generated_trend_descr")]
pub struct GeneratedTrend {
    pub name: PostgresName,
    pub data_type: String,

    #[serde(default="default_empty_string")]
    pub description: String,
    pub expression: String,

    #[serde(default="default_extra_data")]
    pub extra_data: Value,
}

fn default_empty_string() -> String {
    String::new()
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSql)]
#[postgres(name = "trend_store_part_descr")]
pub struct TrendStorePart {
    pub name: PostgresName,
    pub trends: Vec<Trend>,

    #[serde(default="default_generated_trends")]
    pub generated_trends: Vec<GeneratedTrend>,
}

fn default_generated_trends() -> Vec<GeneratedTrend> {
    Vec::new()
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TrendStore {
    pub data_source: String,
    pub entity_type: String,
    #[serde(with = "humantime_serde")]
    pub granularity: Duration,
    #[serde(with = "humantime_serde")]
    pub partition_size: Duration,
    pub parts: Vec<TrendStorePart>,
}

pub fn test_to_generated_trend(conn: &mut Client, generated_trend_descr: &GeneratedTrend) -> Result<String, String> {
    let result = conn.query_one("SELECT ($1::trend_directory.generated_trend_descr).name", &[&generated_trend_descr]).unwrap();
    //let result = conn.query_one("SELECT (($1, $2, '', null, null, null)::trend_directory.trend_descr).data_type", &[&trend.name, &trend.data_type]).unwrap();

    Ok(result.get(0))
}

pub fn test_to_trend_store_part(conn: &mut Client, trend_store_part: &TrendStorePart) -> Result<String, String> {
    let result = conn.query_one("SELECT ($1::trend_directory.trend_store_part).name", &[&trend_store_part]).unwrap();

    Ok(result.get(0))
}

pub fn create_trend_store(conn: &mut Client, trend_store: &TrendStore) -> Option<i32> {
    let query = concat!(
        "SELECT id ",
        "FROM trend_directory.create_trend_store(",
        "$1, $2, $3::integer * interval '1 sec', $4::integer * interval '1 sec', ",
        "$5::trend_directory.trend_store_part_descr[]",
        ")"
    );

    let granularity_seconds: i32 = trend_store.granularity.as_secs() as i32;
    let partition_size_seconds: i32 = trend_store.partition_size.as_secs() as i32;

    let result = conn
        .query_one(
            query,
            &[
                &trend_store.data_source,
                &trend_store.entity_type,
                &granularity_seconds,
                &partition_size_seconds,
                &trend_store.parts,
            ],
        );

    match result {
        Ok(row) => {
            let value: i32 = row.get(0);

            Some(value)
        },
        Err(e) => {
            println!("Error creating trend store: {}", e);
            None
        }
    }
}
