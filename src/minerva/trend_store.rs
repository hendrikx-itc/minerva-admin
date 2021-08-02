use postgres::types::ToSql;
use postgres::{Client, Row};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::convert::From;
use std::fmt;
use std::time::Duration;

use humantime::format_duration;

use super::change::Change;
use super::interval::parse_interval;

type PostgresName = String;

pub struct DeleteTrendStoreError {
    original: String,
    kind: DeleteTrendStoreErrorKind,
}

impl DeleteTrendStoreError {
    fn database_error(e: postgres::Error) -> DeleteTrendStoreError {
        DeleteTrendStoreError {
            original: format!("{}", e),
            kind: DeleteTrendStoreErrorKind::DatabaseError,
        }
    }
}

impl From<postgres::Error> for DeleteTrendStoreError {
    fn from(e: postgres::Error) -> DeleteTrendStoreError {
        DeleteTrendStoreError::database_error(e)
    }
}

impl fmt::Display for DeleteTrendStoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.kind {
            DeleteTrendStoreErrorKind::DatabaseError => {
                write!(f, "database error: {}", self.original)
            }
            DeleteTrendStoreErrorKind::NoSuchTrendStore => {
                write!(f, "no such trend: {}", self.original)
            }
        }
    }
}

enum DeleteTrendStoreErrorKind {
    NoSuchTrendStore,
    DatabaseError,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSql)]
#[postgres(name = "trend_descr")]
pub struct Trend {
    pub name: PostgresName,
    pub data_type: String,
    #[serde(default = "default_empty_string")]
    pub description: String,
    #[serde(default = "default_time_aggregation")]
    pub time_aggregation: String,
    #[serde(default = "default_entity_aggregation")]
    pub entity_aggregation: String,
    #[serde(default = "default_extra_data")]
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

impl fmt::Display for Trend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Trend({}, {})", &self.name, &self.data_type)
    }
}

pub struct AddTrends {
    pub trend_store_part: TrendStorePart,
    pub trends: Vec<Trend>,
}

impl fmt::Display for AddTrends {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AddTrends({}, {:?})",
            &self.trend_store_part, &self.trends
        )
    }
}

impl Change for AddTrends {
    fn apply(&self, client: &mut Client) -> Result<String, String> {
        let query = concat!(
            "SELECT trend_directory.create_table_trends(trend_store_part, $1) ",
            "FROM trend_directory.trend_store_part WHERE name = $2",
        );

        let result = client.query_one(query, &[&self.trends, &self.trend_store_part.name]);

        match result {
            Ok(_row) => Ok(format!("Added {} trends to trend store part '{}'", &self.trends.len(), &self.trend_store_part.name)),
            Err(e) => Err(format!("Error adding trends to trend store part: {}", e)),
        }
    }
}

pub struct ModifyTrendDataType {
    pub trend_name: String,
    pub from_type: String,
    pub to_type: String,
}

impl fmt::Display for ModifyTrendDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Trend({}, {}->{})",
            &self.trend_name, &self.from_type, &self.to_type
        )
    }
}

/// A set of trends of a trend store part for which the data type needs to
/// change.
///
/// The change of data types for multiple trends in a trend store part is
/// grouped into one operation for efficiency purposes.
pub struct ModifyTrendDataTypes {
    pub trend_store_part: TrendStorePart,
    pub modifications: Vec<ModifyTrendDataType>,
}

impl fmt::Display for ModifyTrendDataTypes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let modifications: Vec<String> = self
            .modifications
            .iter()
            .map(|m| format!("{}", m))
            .collect();

        write!(
            f,
            "ModifyTrendDataTypes({}, {})",
            &self.trend_store_part,
            &modifications.join(","),
        )
    }
}

impl Change for ModifyTrendDataTypes {
    fn apply(&self, client: &mut Client) -> Result<String, String> {
        let mut transaction = client.transaction().unwrap();

        let timeout_query = "SET SESSION statement_timeout = 0";

        let result = transaction.execute(timeout_query, &[]);

        match result {
            Ok(_) => (),
            Err(e) => {
                return Err(format!("Error setting session timeout: {}", e));
            }
        }

        let timeout_query = "SET SESSION lock_timeout = '10min'";

        let result = transaction.execute(timeout_query, &[]);

        match result {
            Ok(_) => (),
            Err(e) => {
                return Err(format!("Error setting lock timeout: {}", e));
            }
        }

        let query = concat!(
            "UPDATE trend_directory.table_trend tt ",
            "SET data_type = $1 ",
            "FROM trend_directory.trend_store_part tsp ",
            "WHERE tsp.id = tt.trend_store_part_id AND tsp.name = $2 AND tt.name = $3"
        );

        println!("{}", &query);

        for modification in &self.modifications {
            let result = transaction.execute(
                query,
                &[
                    &modification.to_type,
                    &self.trend_store_part.name,
                    &modification.trend_name,
                ],
            );

            match result {
                Ok(_) => {}
                Err(e) => {
                    transaction.rollback().unwrap();

                    return Err(format!("Error changing data types: {}", e));
                }
            }
        }

        let alter_type_parts: Vec<String> = self
            .modifications
            .iter()
            .map(|m| {
                format!(
                    "ALTER \"{}\" TYPE {} USING CAST(\"{}\" AS {})",
                    &m.trend_name, &m.to_type, &m.trend_name, &m.to_type
                )
            })
            .collect();

        let alter_type_parts_str = alter_type_parts.join(", ");

        let alter_query = format!(
            "ALTER TABLE trend.\"{}\" {}",
            &self.trend_store_part.name, &alter_type_parts_str
        );

        println!("{}", alter_query);

        let alter_query_slice: &str = &alter_query;

        let result = transaction.execute(alter_query_slice, &[]);

        match result {
            Ok(_) => {
                transaction.commit().unwrap();

                Ok(format!("Altered trend data types for trend store part '{}'", &self.trend_store_part.name))
            }
            Err(e) => {
                transaction.rollback().unwrap();

                Err(format!("Error changing data types: {}", e))
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSql)]
#[postgres(name = "generated_trend_descr")]
pub struct GeneratedTrend {
    pub name: PostgresName,
    pub data_type: String,

    #[serde(default = "default_empty_string")]
    pub description: String,
    pub expression: String,

    #[serde(default = "default_extra_data")]
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

    #[serde(default = "default_generated_trends")]
    pub generated_trends: Vec<GeneratedTrend>,
}

fn default_generated_trends() -> Vec<GeneratedTrend> {
    Vec::new()
}

impl TrendStorePart {
    pub fn diff(&self, other: &TrendStorePart) -> Vec<Box<dyn Change>> {
        let mut changes: Vec<Box<dyn Change>> = Vec::new();

        let mut new_trends: Vec<Trend> = Vec::new();
        let mut alter_trend_data_types: Vec<ModifyTrendDataType> = Vec::new();

        for other_trend in &other.trends {
            match self
                .trends
                .iter()
                .find(|my_trend| my_trend.name == other_trend.name)
            {
                Some(my_trend) => {
                    // The trend already exists, check for changes
                    if my_trend.data_type != other_trend.data_type {
                        alter_trend_data_types.push(ModifyTrendDataType {
                            trend_name: my_trend.name.clone(),
                            from_type: my_trend.data_type.clone(),
                            to_type: other_trend.data_type.clone(),
                        });
                    }
                }
                None => {
                    new_trends.push(other_trend.clone());
                }
            }
        }

        if new_trends.len() > 0 {
            changes.push(Box::new(AddTrends {
                trend_store_part: self.clone(),
                trends: new_trends,
            }));
        }

        if alter_trend_data_types.len() > 0 {
            changes.push(Box::new(ModifyTrendDataTypes {
                trend_store_part: self.clone(),
                modifications: alter_trend_data_types,
            }));
        }

        changes
    }
}

impl fmt::Display for TrendStorePart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TrendStorePart({})", &self.name)
    }
}

pub struct AddTrendStorePart {
    trend_store: TrendStore,
    trend_store_part: TrendStorePart,
}

impl Change for AddTrendStorePart {
    fn apply(&self, client: &mut Client) -> Result<String, String> {
        let query = concat!(
            "SELECT trend_directory.create_trend_store_part(trend_store.id, $1) ",
            "FROM trend_directory.trend_store ",
            "JOIN directory.data_source ON data_source.id = trend_store.data_source_id ",
            "JOIN directory.entity_type ON entity_type.id = trend_store.entity_type_id ",
            "WHERE data_source.name = $2 AND entity_type.name = $3 AND granularity = $4::integer * interval '1 sec'",
        );

        let granularity_seconds: i32 = self.trend_store.granularity.as_secs() as i32;

        let result = client.query_one(
            query,
            &[
                &self.trend_store_part.name,
                &self.trend_store.data_source,
                &self.trend_store.entity_type,
                &granularity_seconds,
            ],
        );

        match result {
            Ok(_row) => Ok(format!("Added trend store part '{}' to trend store '{}'", &self.trend_store_part.name, &self.trend_store)),
            Err(e) => Err(format!("Error creating trend store part '{}': {}", &self.trend_store_part.name, e)),
        }
    }
}

impl fmt::Display for AddTrendStorePart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AddTrendStorePart({}, {})", &self.trend_store, &self.trend_store_part)
    }
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

impl TrendStore {
    pub fn diff(&self, other: &TrendStore) -> Vec<Box<dyn Change>> {
        let mut changes: Vec<Box<dyn Change>> = Vec::new();

        for other_part in &other.parts {
            match self
                .parts
                .iter()
                .find(|my_part| my_part.name == other_part.name)
            {
                Some(my_part) => {
                    changes.append(&mut my_part.diff(other_part));
                }
                None => {
                    changes.push(Box::new(AddTrendStorePart {
                        trend_store: self.clone(),
                        trend_store_part: other_part.clone(),
                    }));
                }
            }
        }

        changes
    }
}

impl fmt::Display for TrendStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TrendStore({}, {}, {})",
            &self.data_source,
            &self.entity_type,
            &humantime::format_duration(self.granularity).to_string()
        )
    }
}

pub fn list_trend_stores(conn: &mut Client) -> Result<Vec<String>, String> {
    let query = concat!(
        "SELECT ts.id, ds.name, et.name, ts.granularity::text ",
        "FROM trend_directory.trend_store ts ",
        "JOIN directory.data_source ds ON ds.id = ts.data_source_id ",
        "JOIN directory.entity_type et ON et.id = ts.entity_type_id"
    );

    let result = conn.query(query, &[]).unwrap();

    let trend_stores = result
        .into_iter()
        .map(|row: Row| {
            format!(
                "{} - {} - {} - {}",
                row.get::<usize, i32>(0),
                row.get::<usize, String>(1),
                row.get::<usize, String>(2),
                row.get::<usize, String>(3),
            )
        })
        .collect();

    Ok(trend_stores)
}

pub fn delete_trend_store(conn: &mut Client, id: i32) -> Result<(), DeleteTrendStoreError> {
    let query = "SELECT trend_directory.delete_trend_store($1)";

    let deleted = conn.execute(query, &[&id])?;

    if deleted == 0 {
        Err(DeleteTrendStoreError {
            kind: DeleteTrendStoreErrorKind::NoSuchTrendStore,
            original: String::from("No trend store matches"),
        })
    } else {
        Ok(())
    }
}

pub fn load_trend_store(
    conn: &mut Client,
    data_source: &str,
    entity_type: &str,
    granularity: &Duration,
) -> Result<TrendStore, String> {
    let query = concat!(
        "SELECT trend_store.id, partition_size::text ",
        "FROM trend_directory.trend_store ",
        "JOIN directory.data_source ON data_source.id = trend_store.data_source_id ",
        "JOIN directory.entity_type ON entity_type.id = trend_store.entity_type_id ",
        "WHERE data_source.name = $1 AND entity_type.name = $2 AND granularity = $3::text::interval"
    );

    let granularity_str: String = format_duration(granularity.clone()).to_string();

    let result = conn
        .query_one(query, &[&data_source, &entity_type, &granularity_str])
        .unwrap();

    let parts = load_trend_store_parts(conn, result.get::<usize, i32>(0));

    let partition_size_str = result.get::<usize, String>(1);
    let partition_size = parse_interval(&partition_size_str).unwrap();

    Ok(TrendStore {
        data_source: String::from(data_source),
        entity_type: String::from(entity_type),
        granularity: granularity.clone(),
        partition_size: partition_size.clone(),
        parts: parts,
    })
}

fn load_trend_store_parts(conn: &mut Client, trend_store_id: i32) -> Vec<TrendStorePart> {
    let trend_store_part_query =
        "SELECT id, name FROM trend_directory.trend_store_part WHERE trend_store_id = $1";

    let trend_store_part_result = conn
        .query(trend_store_part_query, &[&trend_store_id])
        .unwrap();

    let mut parts: Vec<TrendStorePart> = Vec::new();

    for trend_store_part_row in trend_store_part_result {
        let trend_store_part_id: i32 = trend_store_part_row.get(0);
        let trend_store_part_name: &str = trend_store_part_row.get(1);

        let trend_query = concat!(
            "SELECT name, data_type, description, entity_aggregation, time_aggregation, extra_data ",
            "FROM trend_directory.table_trend ",
            "WHERE trend_store_part_id = $1",
        );

        let trend_result = conn.query(trend_query, &[&trend_store_part_id]).unwrap();

        let mut trends = Vec::new();

        for trend_row in trend_result {
            let trend_name: &str = trend_row.get(0);
            let trend_data_type: &str = trend_row.get(1);
            let trend_description: &str = trend_row.get(2);
            let trend_entity_aggregation: &str = trend_row.get(3);
            let trend_time_aggregation: &str = trend_row.get(4);
            let trend_extra_data: Value = trend_row.get(5);

            trends.push(Trend {
                name: String::from(trend_name),
                data_type: String::from(trend_data_type),
                description: String::from(trend_description),
                entity_aggregation: String::from(trend_entity_aggregation),
                time_aggregation: String::from(trend_time_aggregation),
                extra_data: trend_extra_data,
            })
        }

        parts.push(TrendStorePart {
            name: String::from(trend_store_part_name),
            trends: trends,
            generated_trends: Vec::new(),
        });
    }

    parts
}

pub fn load_trend_stores(conn: &mut Client) -> Result<Vec<TrendStore>, String> {
    let mut trend_stores: Vec<TrendStore> = Vec::new();

    let query = concat!(
        "SELECT trend_store.id, data_source.name, entity_type.name, granularity::text, partition_size::text ",
        "FROM trend_directory.trend_store ",
        "JOIN directory.data_source ON data_source.id = trend_store.data_source_id ",
        "JOIN directory.entity_type ON entity_type.id = trend_store.entity_type_id"
    );

    let result = conn.query(query, &[]).unwrap();

    for row in result {
        let trend_store_id: i32 = row.get(0);
        let data_source: &str = row.get(1);
        let entity_type: &str = row.get(2);
        let granularity_str: String = row.get(3);
        let partition_size_str: String = row.get(4);
        let parts = load_trend_store_parts(conn, trend_store_id);

        // Hack for humankind parsing compatibility with PostgreSQL interval
        // representation
        let parse_result = parse_interval(&granularity_str);

        let granularity = match parse_result {
            Ok(g) => g,
            Err(e) => {
                return Err(format!(
                    "Error parsing granularity '{}': {}",
                    &granularity_str, e
                ));
            }
        };

        let parse_result = parse_interval(&partition_size_str);

        let partition_size = match parse_result {
            Ok(g) => g,
            Err(e) => {
                return Err(format!(
                    "Error parsing partition size '{}': {}",
                    &partition_size_str, e
                ));
            }
        };

        trend_stores.push(TrendStore {
            data_source: String::from(data_source),
            entity_type: String::from(entity_type),
            granularity: granularity,
            partition_size: partition_size,
            parts: parts,
        });
    }

    Ok(trend_stores)
}

pub struct AddTrendStore {
    pub trend_store: TrendStore,
}

impl fmt::Display for AddTrendStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AddTrendStore({})", &self.trend_store)
    }
}

impl Change for AddTrendStore {
    fn apply(&self, client: &mut Client) -> Result<String, String> {
        let query = concat!(
            "SELECT id ",
            "FROM trend_directory.create_trend_store(",
            "$1, $2, $3::text::interval, $4::text::interval, ",
            "$5::trend_directory.trend_store_part_descr[]",
            ")"
        );

        let granularity_text = humantime::format_duration(self.trend_store.granularity).to_string();
        let partition_size_text =
            humantime::format_duration(self.trend_store.partition_size).to_string();

        let result = client.query_one(
            query,
            &[
                &self.trend_store.data_source,
                &self.trend_store.entity_type,
                &granularity_text,
                &partition_size_text,
                &self.trend_store.parts,
            ],
        );

        match result {
            Ok(_row) => Ok(format!("Added trend store {}", &self.trend_store)),
            Err(e) => Err(format!("Error creating trend store: {}", e)),
        }
    }
}
