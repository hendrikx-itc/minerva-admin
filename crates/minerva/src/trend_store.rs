use postgres_types::Type;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::convert::From;
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, GenericClient, Row};

use humantime::format_duration;

use chrono::{DateTime, Utc};

use async_trait::async_trait;

use super::change::{Change, ChangeResult, GenericChange};
use super::error::{ConfigurationError, DatabaseError, Error, RuntimeError};
use super::interval::parse_interval;

type PostgresName = String;

trait SanityCheck {
    fn check(&self) -> Result<(), String>;
}

pub struct DeleteTrendStoreError {
    original: String,
    kind: DeleteTrendStoreErrorKind,
}

impl DeleteTrendStoreError {
    fn database_error(e: tokio_postgres::Error) -> DeleteTrendStoreError {
        DeleteTrendStoreError {
            original: format!("{e}"),
            kind: DeleteTrendStoreErrorKind::DatabaseError,
        }
    }
}

impl From<tokio_postgres::Error> for DeleteTrendStoreError {
    fn from(e: tokio_postgres::Error) -> DeleteTrendStoreError {
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

impl Trend {
    pub fn sql_type(&self) -> Type {
        match self.data_type.as_str() {
            "integer" => Type::INT4,
            "numeric" => Type::NUMERIC,
            &_ => Type::TEXT,
        }
    }
}

pub struct RemoveTrends {
    pub trend_store_part: TrendStorePart,
    pub trends: Vec<String>,
}

impl fmt::Display for RemoveTrends {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RemoveTrends({}, {})",
            &self.trend_store_part,
            self.trends.len()
        )
    }
}

impl fmt::Debug for RemoveTrends {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RemoveTrends({}, {})",
            &self.trend_store_part,
            &self
                .trends
                .iter()
                .map(|t| format!("'{}'", &t))
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

#[async_trait]
impl GenericChange for RemoveTrends {
    async fn generic_apply<T: GenericClient + Send + Sync>(&self, client: &mut T) -> ChangeResult {
        let query = concat!(
            "SELECT trend_directory.remove_table_trend(table_trend) ",
            "FROM trend_directory.table_trend ",
            "JOIN trend_directory.trend_store_part ON trend_store_part.id = table_trend.trend_store_part_id ",
            "WHERE trend_store_part.name = $1 AND table_trend.name = $2",
        );

        for trend_name in &self.trends {
            client
                .query_one(query, &[&self.trend_store_part.name, &trend_name])
                .await
                .map_err(|e| {
                    DatabaseError::from_msg(format!(
                        "Error removing trend '{}' from trend store part: {}",
                        &trend_name, e
                    ))
                })?;
        }

        Ok(format!(
            "Removed {} trends from trend store part '{}'",
            &self.trends.len(),
            &self.trend_store_part.name
        ))
    }
}

#[async_trait]
impl Change for RemoveTrends {
    async fn apply(&self, client: &mut Client) -> ChangeResult {
        self.generic_apply(client).await
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
            "AddTrends({}, {})",
            &self.trend_store_part,
            self.trends.len()
        )
    }
}

impl fmt::Debug for AddTrends {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AddTrends({}, {})",
            &self.trend_store_part,
            &self
                .trends
                .iter()
                .map(|t| format!("{}", &t))
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

#[async_trait]
impl GenericChange for AddTrends {
    async fn generic_apply<T: GenericClient + Send + Sync>(&self, client: &mut T) -> ChangeResult {
        let query = concat!(
            "SELECT trend_directory.create_table_trends(trend_store_part, $1) ",
            "FROM trend_directory.trend_store_part WHERE name = $2",
        );

        client
            .query_one(query, &[&self.trends, &self.trend_store_part.name])
            .await
            .map_err(|e| {
                DatabaseError::from_msg(format!("Error adding trends to trend store part: {e}"))
            })?;

        Ok(format!(
            "Added {} trends to trend store part '{}'",
            &self.trends.len(),
            &self.trend_store_part.name
        ))
    }
}

#[async_trait]
impl Change for AddTrends {
    async fn apply(&self, client: &mut Client) -> ChangeResult {
        self.generic_apply(client).await
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
        write!(
            f,
            "ModifyTrendDataTypes({}, {})",
            &self.trend_store_part,
            self.modifications.len(),
        )
    }
}

impl fmt::Debug for ModifyTrendDataTypes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let modifications: Vec<String> =
            self.modifications.iter().map(|m| format!("{m}")).collect();

        write!(
            f,
            "ModifyTrendDataTypes({}, {})",
            &self.trend_store_part,
            &modifications.join(", "),
        )
    }
}

#[async_trait]
impl GenericChange for ModifyTrendDataTypes {
    async fn generic_apply<T: GenericClient + Sync + Send>(&self, client: &mut T) -> ChangeResult {
        let transaction = client
            .transaction()
            .await
            .map_err(|e| DatabaseError::from_msg(format!("could not start transaction: {e}")))?;

        let timeout_query = "SET SESSION statement_timeout = 0";

        let result = transaction.execute(timeout_query, &[]).await;

        if let Err(e) = result {
            return Err(
                DatabaseError::from_msg(format!("Error setting session timeout: {e}")).into(),
            );
        }

        let timeout_query = "SET SESSION lock_timeout = '10min'";

        let result = transaction.execute(timeout_query, &[]).await;

        if let Err(e) = result {
            return Err(DatabaseError::from_msg(format!("Error setting lock timeout: {e}")).into());
        }

        let query = concat!(
            "UPDATE trend_directory.table_trend tt ",
            "SET data_type = $1 ",
            "FROM trend_directory.trend_store_part tsp ",
            "WHERE tsp.id = tt.trend_store_part_id AND tsp.name = $2 AND tt.name = $3"
        );

        for modification in &self.modifications {
            let result = transaction
                .execute(
                    query,
                    &[
                        &modification.to_type,
                        &self.trend_store_part.name,
                        &modification.trend_name,
                    ],
                )
                .await;

            if let Err(e) = result {
                transaction.rollback().await.unwrap();

                return Err(
                    DatabaseError::from_msg(format!("Error changing data types: {e}")).into(),
                );
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

        let alter_query_slice: &str = &alter_query;

        if let Err(e) = transaction.execute(alter_query_slice, &[]).await {
            transaction.rollback().await.unwrap();

            return Err(match e.code() {
                Some(code) => DatabaseError::from_msg(format!(
                    "Error changing data types: {} - {}",
                    code.code(),
                    e
                ))
                .into(),
                None => DatabaseError::from_msg(format!("Error changing data types: {e}")).into(),
            });
        }

        if let Err(e) = transaction.commit().await {
            return Err(DatabaseError::from_msg(format!("Error committing changes: {e}")).into());
        }

        Ok(format!(
            "Altered trend data types for trend store part '{}'",
            &self.trend_store_part.name
        ))
    }
}

#[async_trait]
impl Change for ModifyTrendDataTypes {
    async fn apply(&self, client: &mut Client) -> ChangeResult {
        self.generic_apply(client).await
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
    pub fn diff(&self, other: &TrendStorePart) -> Vec<Box<dyn Change + Send>> {
        let mut changes: Vec<Box<dyn Change + Send>> = Vec::new();

        let mut new_trends: Vec<Trend> = Vec::new();
        let mut removed_trends: Vec<String> = Vec::new();
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

        if !new_trends.is_empty() {
            changes.push(Box::new(AddTrends {
                trend_store_part: self.clone(),
                trends: new_trends,
            }));
        }

        for my_trend in &self.trends {
            match other
                .trends
                .iter()
                .find(|other_trend| other_trend.name == my_trend.name)
            {
                Some(_) => {
                    // Ok, the trend still exists
                }
                None => {
                    removed_trends.push(my_trend.name.clone());
                }
            }
        }

        if !removed_trends.is_empty() {
            changes.push(Box::new(RemoveTrends {
                trend_store_part: self.clone(),
                trends: removed_trends,
            }))
        }

        if !alter_trend_data_types.is_empty() {
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

impl SanityCheck for TrendStorePart {
    fn check(&self) -> Result<(), String> {
        Ok(())
    }
}

pub struct AddTrendStorePart {
    pub trend_store: TrendStore,
    pub trend_store_part: TrendStorePart,
}

#[async_trait]
impl GenericChange for AddTrendStorePart {
    async fn generic_apply<T: GenericClient + Send + Sync>(&self, client: &mut T) -> ChangeResult {
        let query = concat!(
            "SELECT trend_directory.create_trend_store_part(trend_store.id, $1) ",
            "FROM trend_directory.trend_store ",
            "JOIN directory.data_source ON data_source.id = trend_store.data_source_id ",
            "JOIN directory.entity_type ON entity_type.id = trend_store.entity_type_id ",
            "WHERE data_source.name = $2 AND entity_type.name = $3 AND granularity = $4::integer * interval '1 sec'",
        );

        let mut granularity_seconds: i32 = self.trend_store.granularity.as_secs() as i32;
        if (granularity_seconds > 2500000) & (granularity_seconds < 3000000) {
            granularity_seconds = 2592000 // rust and postgres disagree on the number of seconds in a month
        }

        client
            .query_one(
                query,
                &[
                    &self.trend_store_part.name,
                    &self.trend_store.data_source,
                    &self.trend_store.entity_type,
                    &granularity_seconds,
                ],
            )
            .await
            .map_err(|e| {
                DatabaseError::from_msg(format!(
                    "Error creating trend store part '{}': {}",
                    &self.trend_store_part.name, e
                ))
            })?;

        Ok(format!(
            "Added trend store part '{}' to trend store '{}'",
            &self.trend_store_part.name, &self.trend_store
        ))
    }
}

#[async_trait]
impl Change for AddTrendStorePart {
    async fn apply(&self, client: &mut Client) -> ChangeResult {
        self.generic_apply(client).await
    }
}

impl fmt::Display for AddTrendStorePart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AddTrendStorePart({}, {})",
            &self.trend_store, &self.trend_store_part
        )
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
    pub fn diff(&self, other: &TrendStore) -> Vec<Box<dyn Change + Send>> {
        let mut changes: Vec<Box<dyn Change + Send>> = Vec::new();

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

pub async fn list_trend_stores(
    conn: &mut Client,
) -> Result<Vec<(i32, String, String, String)>, String> {
    let query = concat!(
        "SELECT ts.id, ds.name, et.name, ts.granularity::text ",
        "FROM trend_directory.trend_store ts ",
        "JOIN directory.data_source ds ON ds.id = ts.data_source_id ",
        "JOIN directory.entity_type et ON et.id = ts.entity_type_id"
    );

    let result = conn.query(query, &[]).await.unwrap();

    let trend_stores = result
        .into_iter()
        .map(|row: Row| {
            (
                row.get::<usize, i32>(0),
                row.get::<usize, String>(1),
                row.get::<usize, String>(2),
                row.get::<usize, String>(3),
            )
        })
        .collect();

    Ok(trend_stores)
}

pub async fn delete_trend_store(conn: &mut Client, id: i32) -> Result<(), DeleteTrendStoreError> {
    let query = "SELECT trend_directory.delete_trend_store($1)";

    let deleted = conn.execute(query, &[&id]).await?;

    if deleted == 0 {
        Err(DeleteTrendStoreError {
            kind: DeleteTrendStoreErrorKind::NoSuchTrendStore,
            original: String::from("No trend store matches"),
        })
    } else {
        Ok(())
    }
}

pub async fn get_trend_store_id<T: GenericClient>(
    conn: &mut T,
    trend_store: &TrendStore
) -> Result<i32, Error> {
    let query = concat!(
        "SELECT trend_store.id ",
        "FROM trend_directory.trend_store ",
        "JOIN directory.data_source ON data_source.id = trend_store.data_source_id ",
        "JOIN directory.entity_type ON entity_type.id = trend_store.entity_type_id ",
        "WHERE data_source.name = $1 AND entity_type.name = $2 AND granularity = $3::text::interval"
    );

    let granularity_str: String = format_duration(trend_store.granularity).to_string();

    let result = conn
        .query_one(query, &[&trend_store.data_source, &trend_store.entity_type, &granularity_str])
        .await?;

    let trend_store_id = result.get::<usize, i32>(0);

    Ok(trend_store_id)
}

pub async fn load_trend_store<T: GenericClient>(
    conn: &mut T,
    data_source: &str,
    entity_type: &str,
    granularity: &Duration,
) -> Result<TrendStore, Error> {
    let query = concat!(
        "SELECT trend_store.id, partition_size::text ",
        "FROM trend_directory.trend_store ",
        "JOIN directory.data_source ON data_source.id = trend_store.data_source_id ",
        "JOIN directory.entity_type ON entity_type.id = trend_store.entity_type_id ",
        "WHERE data_source.name = $1 AND entity_type.name = $2 AND granularity = $3::text::interval"
    );

    let granularity_str: String = format_duration(*granularity).to_string();

    let result = conn
        .query_one(query, &[&data_source, &entity_type, &granularity_str])
        .await?;

    let parts = load_trend_store_parts(conn, result.get::<usize, i32>(0)).await;

    let partition_size_str = result.get::<usize, String>(1);
    let partition_size = parse_interval(&partition_size_str).unwrap();

    Ok(TrendStore {
        data_source: String::from(data_source),
        entity_type: String::from(entity_type),
        granularity: *granularity,
        partition_size,
        parts,
    })
}

async fn load_trend_store_parts<T: GenericClient>(
    conn: &mut T,
    trend_store_id: i32,
) -> Vec<TrendStorePart> {
    let trend_store_part_query =
        "SELECT id, name FROM trend_directory.trend_store_part WHERE trend_store_id = $1";

    let trend_store_part_result = conn
        .query(trend_store_part_query, &[&trend_store_id])
        .await
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

        let trend_result = conn
            .query(trend_query, &[&trend_store_part_id])
            .await
            .unwrap();

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
            trends,
            generated_trends: Vec::new(),
        });
    }

    parts
}

pub async fn load_trend_stores(conn: &mut Client) -> Result<Vec<TrendStore>, Error> {
    let mut trend_stores: Vec<TrendStore> = Vec::new();

    let query = concat!(
        "SELECT trend_store.id, data_source.name, entity_type.name, granularity::text, partition_size::text ",
        "FROM trend_directory.trend_store ",
        "JOIN directory.data_source ON data_source.id = trend_store.data_source_id ",
        "JOIN directory.entity_type ON entity_type.id = trend_store.entity_type_id"
    );

    let result = conn.query(query, &[]).await.unwrap();

    for row in result {
        let trend_store_id: i32 = row.get(0);
        let data_source: &str = row.get(1);
        let entity_type: &str = row.get(2);
        let granularity_str: String = row.get(3);
        let partition_size_str: String = row.get(4);
        let parts = load_trend_store_parts(conn, trend_store_id).await;

        // Hack for humankind parsing compatibility with PostgreSQL interval
        // representation
        let granularity = parse_interval(&granularity_str).map_err(|e| {
            RuntimeError::from_msg(format!(
                "Error parsing granularity '{}': {}",
                &granularity_str, e
            ))
        })?;

        let partition_size = parse_interval(&partition_size_str).map_err(|e| {
            RuntimeError::from_msg(format!(
                "Error parsing partition size '{}': {}",
                &partition_size_str, e
            ))
        })?;

        trend_stores.push(TrendStore {
            data_source: String::from(data_source),
            entity_type: String::from(entity_type),
            granularity,
            partition_size,
            parts,
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

#[async_trait]
impl GenericChange for AddTrendStore {
    async fn generic_apply<T: GenericClient + Sync + Send>(&self, client: &mut T) -> ChangeResult {
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

        client
            .query_one(
                query,
                &[
                    &self.trend_store.data_source,
                    &self.trend_store.entity_type,
                    &granularity_text,
                    &partition_size_text,
                    &self.trend_store.parts,
                ],
            )
            .await
            .map_err(|e| DatabaseError::from_msg(format!("Error creating trend store: {e}")))?;

        Ok(format!("Added trend store {}", &self.trend_store))
    }
}

#[async_trait]
impl Change for AddTrendStore {
    async fn apply(&self, client: &mut Client) -> ChangeResult {
        self.generic_apply(client).await
    }
}

pub fn load_trend_store_from_file(path: &PathBuf) -> Result<TrendStore, Error> {
    let f = std::fs::File::open(path).map_err(|e| {
        ConfigurationError::from_msg(format!(
            "Could not open trend store definition file '{}': {}",
            path.display(),
            e
        ))
    })?;

    if path.extension() == Some(std::ffi::OsStr::new("yaml")) {
        let trend_store: TrendStore = serde_yaml::from_reader(f).map_err(|e| {
            RuntimeError::from_msg(format!(
                "Could not read trend store definition from file '{}': {}",
                path.display(),
                e
            ))
        })?;

        Ok(trend_store)
    } else if path.extension() == Some(std::ffi::OsStr::new("json")) {
        let trend_store: TrendStore = serde_json::from_reader(f).map_err(|e| {
            RuntimeError::from_msg(format!(
                "Could not read trend store definition from file '{}': {}",
                path.display(),
                e
            ))
        })?;

        Ok(trend_store)
    } else {
        return Err(ConfigurationError::from_msg(format!(
            "Unsupported trend store definition format '{}'",
            path.extension().unwrap().to_string_lossy()
        ))
        .into());
    }
}

/// Create partitions for the full retention period of all trend stores.
pub async fn create_partitions(
    client: &mut Client,
    ahead_interval: Option<Duration>,
) -> Result<(), Error> {
    let ahead_interval = match ahead_interval {
        Some(i) => i,
        None => humantime::parse_duration("3days").unwrap(),
    };

    let query = concat!("SELECT id FROM trend_directory.trend_store");

    let result = client
        .query(query, &[])
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error loading trend store Ids: {e}")))?;

    for row in result {
        let trend_store_id: i32 = row.get(0);

        create_partitions_for_trend_store(client, trend_store_id, ahead_interval).await?;
    }

    Ok(())
}

pub async fn create_partitions_for_timestamp(
    client: &mut Client,
    timestamp: DateTime<Utc>,
) -> Result<(), Error> {
    let query = concat!("SELECT id FROM trend_directory.trend_store");

    let result = client
        .query(query, &[])
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error loading trend store Ids: {e}")))?;

    for row in result {
        let trend_store_id: i32 = row.get(0);

        create_partitions_for_trend_store_and_timestamp(client, trend_store_id, timestamp).await?;
    }

    Ok(())
}

pub async fn create_partitions_for_trend_store(
    client: &mut Client,
    trend_store_id: i32,
    ahead_interval: Duration,
) -> Result<(), Error> {
    println!("Creating partitions for trend store {}", &trend_store_id);

    let query = concat!(
        "WITH partition_indexes AS (",
        "SELECT trend_directory.timestamp_to_index(partition_size, t) AS i, p.id AS part_id, p.name AS part_name ",
        "FROM trend_directory.trend_store ",
        "JOIN trend_directory.trend_store_part p ON p.trend_store_id = trend_store.id ",
        "JOIN generate_series(now() - partition_size - trend_store.retention_period, now() + partition_size + $2::text::interval, partition_size) t ON true ",
        "WHERE trend_store.id = $1",
        ") ",
        "SELECT partition_indexes.part_id, partition_indexes.part_name, partition_indexes.i FROM partition_indexes ",
        "LEFT JOIN trend_directory.partition ON partition.index = i AND partition.trend_store_part_id = partition_indexes.part_id ",
        "WHERE partition.id IS NULL",
    );

    let ahead_interval_str = humantime::format_duration(ahead_interval).to_string();

    let result = client
        .query(query, &[&trend_store_id, &ahead_interval_str])
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error loading trend store Ids: {e}")))?;

    for row in result {
        let trend_store_part_id: i32 = row.get(0);
        let part_name: String = row.get(1);
        let partition_index: i32 = row.get(2);

        let partition_name =
            create_partition_for_trend_store_part(client, trend_store_part_id, partition_index)
                .await?;

        println!(
            "Created partition for '{}': '{}'",
            &part_name, &partition_name
        );
    }

    Ok(())
}

pub async fn create_partitions_for_trend_store_and_timestamp(
    client: &mut Client,
    trend_store_id: i32,
    timestamp: DateTime<Utc>,
) -> Result<(), Error> {
    println!("Creating partitions for trend store {}", &trend_store_id);

    let query = concat!(
        "WITH partition_indexes AS (",
        "SELECT trend_directory.timestamp_to_index(partition_size, $2) AS i, p.id AS part_id, p.name AS part_name ",
        "FROM trend_directory.trend_store ",
        "JOIN trend_directory.trend_store_part p ON p.trend_store_id = trend_store.id ",
        "WHERE trend_store.id = $1",
        ") ",
        "SELECT partition_indexes.part_id, partition_indexes.part_name, partition_indexes.i FROM partition_indexes ",
        "LEFT JOIN trend_directory.partition ON partition.index = i AND partition.trend_store_part_id = partition_indexes.part_id ",
        "WHERE partition.id IS NULL",
    );

    let result = client
        .query(query, &[&trend_store_id, &timestamp])
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error loading trend store Ids: {e}")))?;

    for row in result {
        let trend_store_part_id: i32 = row.get(0);
        let part_name: String = row.get(1);
        let partition_index: i32 = row.get(2);

        let partition_name =
            create_partition_for_trend_store_part(client, trend_store_part_id, partition_index)
                .await?;

        println!(
            "Created partition for '{}': '{}'",
            &part_name, &partition_name
        );
    }

    Ok(())
}

async fn create_partition_for_trend_store_part(
    client: &mut Client,
    trend_store_part_id: i32,
    partition_index: i32,
) -> Result<String, Error> {
    let query = concat!(
        "SELECT p.name, (trend_directory.create_partition(p, $2::integer)).name ",
        "FROM trend_directory.trend_store_part p ",
        "WHERE p.id = $1",
    );

    let result = client
        .query_one(query, &[&trend_store_part_id, &partition_index])
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error creating partition: {e}")))?;

    let partition_name = result.get(1);

    Ok(partition_name)
}

pub struct TrendStat {
    pub name: String,
    pub max_value: Option<String>,
    pub min_value: Option<String>,
}

pub struct AnalyzeResult {
    pub trend_stats: Vec<TrendStat>,
}

pub async fn analyze_trend_store_part(
    client: &mut Client,
    name: &str,
) -> Result<AnalyzeResult, Error> {
    let query = "SELECT tt.name FROM trend_directory.trend_store_part tsp JOIN trend_directory.table_trend tt ON tt.trend_store_part_id = tsp.id WHERE tsp.name = $1";

    let result = client.query(query, &[&name]).await.map_err(|e| {
        DatabaseError::from_msg(format!(
            "Could read trends for trend store part '{name}': {e}"
        ))
    })?;

    let trend_names: Vec<String> = result.iter().map(|row| row.get(0)).collect();

    let max_expressions: Vec<String> = trend_names
        .iter()
        .map(|name| format!("max(\"{name}\")::text"))
        .collect();

    let max_expressions_part = max_expressions.join(", ");

    let query = format!(
        "SELECT {} FROM trend.\"{}\" p ",
        &max_expressions_part, name
    );

    let row = client.query_one(&query, &[]).await.map_err(|e| {
        DatabaseError::from_msg(format!("Could not analyze trend store part '{name}': {e}"))
    })?;

    let trend_stats = trend_names
        .iter()
        .enumerate()
        .map(|(i, name)| TrendStat {
            name: name.clone(),
            max_value: row.get(i),
            min_value: None,
        })
        .collect();

    let result = AnalyzeResult { trend_stats };

    Ok(result)
}
