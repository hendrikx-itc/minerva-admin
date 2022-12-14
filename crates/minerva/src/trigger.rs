use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use postgres_types::ToSql;
use serde::{Deserialize, Serialize};

use chrono::{DateTime, Timelike, TimeZone};
use postgres_protocol::escape::{escape_identifier, escape_literal};
use tokio_postgres::{Client, GenericClient, Row};

use async_trait::async_trait;

use crate::interval::parse_interval;

use super::change::{Change, ChangeResult, GenericChange};
use super::error::{ConfigurationError, DatabaseError, Error, RuntimeError};

type PostgresName = String;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KPIDataColumn {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Threshold {
    pub name: String,
    pub data_type: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TrendStoreLink {
    pub part_name: String,
    pub mapping_function: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MappingFunction {
    pub name: String,
    pub source: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Trigger {
    pub name: PostgresName,
    pub kpi_data: Vec<KPIDataColumn>,
    pub kpi_function: String,
    pub thresholds: Vec<Threshold>,
    pub condition: String,
    pub weight: String,
    pub notification: String,
    pub tags: Vec<String>,
    pub fingerprint: String,
    pub notification_store: String,
    pub data: String,
    pub trend_store_links: Vec<TrendStoreLink>,
    pub mapping_functions: Vec<MappingFunction>,
    #[serde(with = "humantime_serde")]
    pub granularity: Duration,
}

impl fmt::Display for Trigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Trigger({})", &self.name,)
    }
}

pub async fn list_triggers(
    conn: &mut Client,
) -> Result<Vec<(String, String, String, String)>, String> {
    let query = concat!(
        "SELECT name, ns::text, granularity::text, default_interval::text, enabled ",
        "FROM trigger.rule ",
        "LEFT JOIN notification_directory.notification_store ns ON ns.id = notification_store_id",
    );

    let result = conn.query(query, &[]).await.unwrap();

    let triggers: Result<Vec<(String, String, String, String)>, String> = result
        .into_iter()
        .map(|row: Row| {
            let name: String = row
                .try_get(0)
                .map_err(|e| format!("could not retrieve name: {}", e))?;
            let notification_store: Option<String> = row
                .try_get(1)
                .map_err(|e| format!("could not retrieve notification store name: {}", e))?;
            let granularity: Option<String> = row
                .try_get(2)
                .map_err(|e| format!("could not retrieve granularity: {}", e))?;
            let default_interval: Option<String> = row
                .try_get(3)
                .map_err(|e| format!("could not retrieve default interval: {}", e))?;

            let trigger_row = (
                name,
                notification_store.unwrap_or("UNDEFINED".into()),
                granularity.unwrap_or("UNDEFINED".into()),
                default_interval.unwrap_or("UNDEFINED".into()),
            );
            Ok(trigger_row)
        })
        .collect();

    triggers
}

pub struct AddTrigger {
    pub trigger: Trigger,
    pub verify: bool,
}

impl fmt::Display for AddTrigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AddTrigger({})", &self.trigger)
    }
}

#[async_trait]
impl GenericChange for AddTrigger {
    async fn generic_apply<T: GenericClient + Sync + Send>(&self, client: &mut T) -> ChangeResult {
        let mut transaction = client.transaction().await?;

        create_type(&self.trigger, &mut transaction).await?;

        create_kpi_function(&self.trigger, &mut transaction).await?;

        create_rule(&self.trigger, &mut transaction).await?;

        set_weight(&self.trigger, &mut transaction).await?;

        set_thresholds(&self.trigger, &mut transaction).await?;

        set_condition(&self.trigger, &mut transaction).await?;

        define_notification_message(&self.trigger, &mut transaction).await?;

        define_notification_data(&self.trigger, &mut transaction).await?;

        create_mapping_functions(&self.trigger, &mut transaction).await?;

        link_trend_stores(&self.trigger, &mut transaction).await?;

        let mut check_result: String = "No check has run".to_string();

        if self.verify {
            check_result = run_checks(&self.trigger.name, &mut transaction).await?;
        }

        transaction.commit().await?;

        let message = match self.verify {
            false => format!("Created trigger '{}'", &self.trigger.name),
            true => format!("Created trigger '{}': {}", &self.trigger.name, check_result),
        };

        Ok(message)

    }
}

async fn create_type<T: GenericClient + Sync + Send>(
    trigger: &Trigger,
    client: &mut T,
) -> ChangeResult {
    let type_name = format!("{}_kpi", &trigger.name);
    let mut cols: Vec<(String, String)> = vec![
        (String::from("entity_id"), String::from("integer")),
        (
            String::from("timestamp"),
            String::from("timestamp with time zone"),
        ),
    ];

    for data_column in trigger.kpi_data.iter() {
        cols.push((data_column.name.clone(), data_column.data_type.clone()))
    }

    let column_spec = cols
        .iter()
        .map(|(name, data_type)| format!("{} {}", escape_identifier(&name), &data_type))
        .collect::<Vec<String>>()
        .join(", ");

    let query = format!(
        "CREATE TYPE trigger_rule.{} AS ({})",
        escape_identifier(&type_name),
        &column_spec,
    );

    client
        .execute(&query, &[])
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error creating KPI type: {}", e)))?;

    Ok(format!("Added KPI type for trigger '{}'", &trigger.name))
}

async fn cleanup_rule<T: GenericClient + Sync + Send>(
    trigger: &Trigger,
    client: &mut T,
) -> ChangeResult {
    let query = "SELECT trigger.cleanup_rule(rule) FROM trigger.rule WHERE name = $1";

    client
        .execute(query, &[&trigger.name])
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error cleaning up rule: {}", e)))?;

    Ok(format!("Cleaned up rule for trigger '{}'", &trigger.name))
}

async fn create_kpi_function<T: GenericClient + Sync + Send>(
    trigger: &Trigger,
    client: &mut T,
) -> ChangeResult {
    let function_name = format!("{}_kpi", &trigger.name);
    let type_name = format!("{}_kpi", &trigger.name);

    let query = format!(
        "CREATE FUNCTION trigger_rule.{}(timestamp with time zone) RETURNS SETOF trigger_rule.{} AS $trigger${}$trigger$ LANGUAGE plpgsql STABLE;",
        &escape_identifier(&function_name),
        &escape_identifier(&type_name),
        &trigger.kpi_function,
    );

    client
        .execute(&query, &[])
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error creating KPI function: {}", e)))?;

    Ok(format!(
        "Added KPI function for trigger '{}'",
        &trigger.name
    ))
}

async fn create_rule<T: GenericClient + Sync + Send>(
    trigger: &Trigger,
    client: &mut T,
) -> ChangeResult {
    let query = format!(
        "SELECT * FROM trigger.create_rule($1, array[{}]::trigger.threshold_def[])",
        trigger
            .thresholds
            .iter()
            .map(|threshold| {
                format!(
                    "({}, {})",
                    escape_literal(&threshold.name),
                    escape_literal(&threshold.data_type)
                )
            })
            .collect::<Vec<String>>()
            .join(",")
    );

    client
        .execute(&query, &[&trigger.name])
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error creating rule: {}", e)))?;

    let query = concat!(
        "UPDATE trigger.rule ",
        "SET notification_store_id = notification_store.id, ",
        "granularity = $1::text::interval ",
        "FROM notification_directory.notification_store ",
        "JOIN directory.data_source ",
        "ON data_source.id = notification_store.data_source_id ",
        "WHERE rule.name = $2 AND data_source.name = $3",
    );

    client
        .execute(
            query,
            &[
                &humantime::format_duration(trigger.granularity).to_string(),
                &trigger.name,
                &trigger.notification_store,
            ],
        )
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error creating rule: {}", e)))?;

    Ok(format!("Added rule for trigger '{}'", &trigger.name))
}

async fn setup_rule<T: GenericClient + Sync + Send>(
    trigger: &Trigger,
    client: &mut T,
) -> ChangeResult {
    let query = format!(
        "SELECT trigger.setup_rule(rule, array[{}]::trigger.threshold_def[]) FROM trigger.rule WHERE name = $1",
        trigger.thresholds.iter().map(|threshold| { format!("({}, {})", escape_literal(&threshold.name), escape_literal(&threshold.data_type)) }).collect::<Vec<String>>().join(",")
    );

    client
        .execute(&query, &[&trigger.name])
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error setting up rule: {}", e)))?;

    let query = concat!(
        "UPDATE trigger.rule ",
        "SET notification_store_id = notification_store.id, ",
        "granularity = $1::text::interval ",
        "FROM notification_directory.notification_store ",
        "JOIN directory.data_source ",
        "ON data_source.id = notification_store.data_source_id ",
        "WHERE rule.name = $2 AND data_source.name = $3",
    );

    client
        .execute(
            query,
            &[
                &humantime::format_duration(trigger.granularity).to_string(),
                &trigger.name,
                &trigger.notification_store,
            ],
        )
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error creating rule: {}", e)))?;

    Ok(format!("Added rule for trigger '{}'", &trigger.name))
}

async fn set_weight<T: GenericClient + Sync + Send>(
    trigger: &Trigger,
    client: &mut T,
) -> ChangeResult {
    let query = format!("SELECT trigger.set_weight($1::name, $2::text)",);

    client
        .execute(&query, &[&trigger.name, &trigger.weight])
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error setting weight: {}", e)))?;

    Ok(format!("Set weight for trigger '{}'", &trigger.name))
}

async fn set_thresholds<T: GenericClient + Sync + Send>(
    trigger: &Trigger,
    client: &mut T,
) -> ChangeResult {
    let function_name = format!("{}_set_thresholds", &trigger.name);
    let function_args = trigger
        .thresholds
        .iter()
        .map(|threshold| threshold.value.clone())
        .collect::<Vec<String>>()
        .join(",");

    let query = format!(
        "SELECT trigger_rule.{}({})",
        &escape_identifier(&function_name),
        function_args,
    );

    client
        .execute(&query, &[])
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error setting thresholds: {}", e)))?;

    Ok(format!("Set thresholds for trigger '{}'", &trigger.name))
}

async fn set_condition<T: GenericClient + Sync + Send>(
    trigger: &Trigger,
    client: &mut T,
) -> ChangeResult {
    let query = "SELECT trigger.set_condition(rule, $1) FROM trigger.rule WHERE name = $2";

    client
        .execute(query, &[&trigger.condition, &trigger.name])
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error setting condition: {}", e)))?;

    Ok(format!("Set condition for trigger '{}'", &trigger.name))
}

async fn define_notification_message<T: GenericClient + Sync + Send>(
    trigger: &Trigger,
    client: &mut T,
) -> ChangeResult {
    let query = "SELECT trigger.define_notification_message($1, $2)";

    client
        .execute(query, &[&trigger.name, &trigger.notification])
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error setting message: {}", e)))?;

    Ok(format!("Set message for trigger '{}'", &trigger.name))
}

async fn define_notification_data<T: GenericClient + Sync + Send>(
    trigger: &Trigger,
    client: &mut T,
) -> ChangeResult {
    let query = "SELECT trigger.define_notification_data($1, $2)";

    client
        .execute(query, &[&trigger.name, &trigger.data])
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error setting data: {}", e)))?;

    Ok(format!("Set data for trigger '{}'", &trigger.name))
}

async fn drop_notification_data_function<T: GenericClient + Sync + Send>(
    trigger: &Trigger,
    client: &mut T,
) -> ChangeResult {
    let function_name = format!("{}_notification_data", &trigger.name);

    let query = format!(
        "DROP FUNCTION IF EXISTS trigger_rule.{}(timestamp with time zone);",
        &escape_identifier(&function_name),
    );

    client
        .execute(&query, &[])
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error dropping data function: {}", e)))?;

    Ok(format!(
        "Dropped data function for trigger '{}'",
        &trigger.name
    ))
}

async fn create_mapping_functions<T: GenericClient + Sync + Send>(
    trigger: &Trigger,
    client: &mut T,
) -> ChangeResult {
    for mapping_function in trigger.mapping_functions.iter() {
        let query = format!(
            "CREATE FUNCTION trend.{}(timestamp with time zone) RETURNS SETOF timestamp with time zone AS $${}$$ LANGUAGE sql STABLE",
            escape_identifier(&mapping_function.name),
            &mapping_function.source,
        );

        client.execute(&query, &[]).await.map_err(|e| {
            DatabaseError::from_msg(format!("Error creating mapping function: {}", e))
        })?;
    }

    Ok(format!(
        "Created mapping functions for trigger '{}'",
        &trigger.name
    ))
}

async fn link_trend_stores<T: GenericClient + Sync + Send>(
    trigger: &Trigger,
    client: &mut T,
) -> ChangeResult {
    for trend_store_link in trigger.trend_store_links.iter() {
        let mapping_function = format!(
            "trend.{}(timestamp with time zone)",
            escape_identifier(&trend_store_link.mapping_function),
        );

        let query = concat!(
            "INSERT INTO trigger.rule_trend_store_link(",
            "rule_id, trend_store_part_id, timestamp_mapping_func",
            ") ",
            "SELECT rule.id, trend_store_part.id, $1::text::regprocedure ",
            "FROM trigger.rule, trend_directory.trend_store_part ",
            "WHERE rule.name = $2 AND trend_store_part.name = $3",
        );

        client
            .execute(
                query,
                &[
                    &mapping_function,
                    &trigger.name,
                    &trend_store_link.part_name,
                ],
            )
            .await
            .map_err(|e| DatabaseError::from_msg(format!("Error linking trend store: {}", e)))?;
    }

    Ok(format!(
        "Linked trend stores for trigger '{}'",
        &trigger.name
    ))
}

/// Truncate a reference timestamp to the nearest timestamp for a specified granularity.
fn truncate_timestamp_for_granularity<Tz>(
    granularity: Duration,
    ref_timestamp: &DateTime<Tz>,
) -> Result<DateTime<Tz>, Error>
where
    Tz: chrono::TimeZone,
{
    match granularity.as_secs() {
        900 => {
            let date = ref_timestamp.date_naive();

            let gran_minutes: u32 = 15;

            let time = ref_timestamp.time();

            let remainder = time.minute() % gran_minutes;

            let minutes = time.minute() - remainder;

            let timestamp = date
                .and_hms_opt(ref_timestamp.time().hour(), minutes, 0)
                .unwrap()
                .and_local_timezone(ref_timestamp.timezone())
                .unwrap();

            Ok(timestamp)
        }
        3600 => {
            let date = ref_timestamp.date_naive();

            let timestamp = date
                .and_hms_opt(ref_timestamp.time().hour(), 0, 0)
                .unwrap()
                .and_local_timezone(ref_timestamp.timezone())
                .unwrap();

            Ok(timestamp)
        }
        86400 => {
            let date = ref_timestamp.date_naive();

            let timestamp = date
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_local_timezone(ref_timestamp.timezone())
                .unwrap();

            Ok(timestamp)
        }
        _ => Err(Error::Runtime(RuntimeError::from_msg(
            format!(
                "Unsupported granularity: {}",
                &humantime::format_duration(granularity)
            )
            .to_string(),
        ))),
    }
}

async fn run_checks<T: GenericClient + Sync + Send>(
    trigger_name: &str,
    client: &mut T,
) -> ChangeResult {
    let trigger = load_trigger(client, trigger_name).await?;

    let query = format!(
        "SELECT * FROM trigger_rule.{}($1::timestamptz)",
        escape_identifier(trigger_name)
    );

    let reference_timestamp = chrono::offset::Local::now();

    let check_timestamp = truncate_timestamp_for_granularity(trigger.granularity, &reference_timestamp)?;

    client
        .execute(&query, &[&check_timestamp])
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error running check: {}", e)))?;

    Ok(format!(
        "Checks run successfully for '{}': '{}'",
        trigger_name, &check_timestamp
    ))
}
async fn unlink_trend_stores<T: GenericClient + Sync + Send>(
    trigger: &Trigger,
    client: &mut T,
) -> ChangeResult {
    let query = "DELETE FROM trigger.rule_trend_store_link USING trigger.rule WHERE rule_id = rule.id AND rule.name = $1";

    client
        .execute(query, &[&trigger.name])
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error unlinking trend stores: {}", e)))?;

    Ok(format!(
        "Unlinked trend stores for trigger '{}'",
        &trigger.name
    ))
}

#[async_trait]
impl Change for AddTrigger {
    async fn apply(&self, client: &mut Client) -> ChangeResult {
        self.generic_apply(client).await
    }
}

pub struct DeleteTrigger {
    pub trigger_name: String,
}

impl fmt::Display for DeleteTrigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DeleteTrigger({})", &self.trigger_name)
    }
}

#[async_trait]
impl GenericChange for DeleteTrigger {
    async fn generic_apply<T: GenericClient + Sync + Send>(&self, client: &mut T) -> ChangeResult {
        let row = client
            .query_one(
                "SELECT count(*) FROM trigger.rule WHERE name = $1",
                &[&self.trigger_name],
            )
            .await
            .map_err(|e| {
                DatabaseError::from_msg(format!("Error checking for rule existance: {}", e))
            })?;

        let count: i64 = row.get(0);

        if count == 0 {
            return Err(Error::Runtime(RuntimeError::from_msg(format!(
                "No trigger found matching name '{}'",
                &self.trigger_name
            ))));
        }

        client
            .execute("SELECT trigger.delete_rule($1)", &[&self.trigger_name])
            .await
            .map_err(|e| DatabaseError::from_msg(format!("Error deleting rule: {}", e)))?;

        Ok(format!("Removed trigger '{}'", &self.trigger_name))
    }
}

#[async_trait]
impl Change for DeleteTrigger {
    async fn apply(&self, client: &mut Client) -> ChangeResult {
        self.generic_apply(client).await
    }
}

pub fn load_trigger_from_file(path: &PathBuf) -> Result<Trigger, Error> {
    let f = std::fs::File::open(path).map_err(|e| {
        ConfigurationError::from_msg(format!(
            "Could not open trigger definition file '{}': {}",
            path.display(),
            e
        ))
    })?;

    if path.extension() == Some(std::ffi::OsStr::new("yaml")) {
        let trigger: Trigger = serde_yaml::from_reader(f).map_err(|e| {
            RuntimeError::from_msg(format!(
                "Could not read trigger definition from file '{}': {}",
                path.display(),
                e
            ))
        })?;

        Ok(trigger)
    } else if path.extension() == Some(std::ffi::OsStr::new("json")) {
        let trigger: Trigger = serde_json::from_reader(f).map_err(|e| {
            RuntimeError::from_msg(format!(
                "Could not read trigger definition from file '{}': {}",
                path.display(),
                e
            ))
        })?;

        Ok(trigger)
    } else {
        return Err(ConfigurationError::from_msg(format!(
            "Unsupported trigger definition format '{}'",
            path.extension().unwrap().to_string_lossy()
        ))
        .into());
    }
}

pub struct UpdateTrigger {
    pub trigger: Trigger,
    pub verify: bool,
}

impl fmt::Display for UpdateTrigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UpdateTrigger({})", &self.trigger)
    }
}

#[async_trait]
impl GenericChange for UpdateTrigger {
    async fn generic_apply<T: GenericClient + Sync + Send>(&self, client: &mut T) -> ChangeResult {
        let mut transaction = client.transaction().await?;

        // Tear down
        drop_notification_data_function(&self.trigger, &mut transaction).await?;

        unlink_trend_stores(&self.trigger, &mut transaction).await?;

        cleanup_rule(&self.trigger, &mut transaction).await?;

        // Build up

        create_type(&self.trigger, &mut transaction).await?;

        create_kpi_function(&self.trigger, &mut transaction).await?;

        setup_rule(&self.trigger, &mut transaction).await?;

        set_weight(&self.trigger, &mut transaction).await?;

        set_thresholds(&self.trigger, &mut transaction).await?;

        set_condition(&self.trigger, &mut transaction).await?;

        define_notification_message(&self.trigger, &mut transaction).await?;

        define_notification_data(&self.trigger, &mut transaction).await?;

        create_mapping_functions(&self.trigger, &mut transaction).await?;

        link_trend_stores(&self.trigger, &mut transaction).await?;

        let mut check_result: String = "No check has run".to_string();

        if self.verify {
            check_result = run_checks(&self.trigger.name, &mut transaction).await?;
        }

        transaction.commit().await?;

        let message = match self.verify {
            false => format!("Updated trigger '{}'", &self.trigger.name),
            true => format!("Updated trigger '{}': {}", &self.trigger.name, check_result),
        };

        Ok(message)
    }
}

#[async_trait]
impl Change for UpdateTrigger {
    async fn apply(&self, client: &mut Client) -> ChangeResult {
        self.generic_apply(client).await
    }
}

pub struct VerifyTrigger {
    pub trigger_name: String,
}

impl fmt::Display for VerifyTrigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VerifyTrigger({})", &self.trigger_name)
    }
}

#[async_trait]
impl GenericChange for VerifyTrigger {
    async fn generic_apply<T: GenericClient + Sync + Send>(&self, client: &mut T) -> ChangeResult {
        let mut transaction = client.transaction().await?;

        let message = run_checks(&self.trigger_name, &mut transaction).await?;

        Ok(message)
    }
}

#[async_trait]
impl Change for VerifyTrigger {
    async fn apply(&self, client: &mut Client) -> ChangeResult {
        self.generic_apply(client).await
    }
}

pub async fn load_trigger<T: GenericClient + Send + Sync>(
    conn: &mut T,
    name: &str,
) -> Result<Trigger, Error> {
    let query = "SELECT name, granularity::text FROM trigger.rule WHERE name = $1";

    let row = conn.query_one(query, &[&String::from(name)]).await.map_err(|e| {
        DatabaseError::from_msg(format!("Error loading trend materializations: {}", e))
    })?;

    let granularity_str: String = row.get(1);

    let granularity = parse_interval(&granularity_str).unwrap();

    Ok(Trigger {
        name: String::from(name),
        condition: String::from(""),
        data: String::from(""),
        fingerprint: String::from(""),
        granularity: granularity,
        kpi_data: Vec::<KPIDataColumn>::new(),
        kpi_function: String::from(""),
        mapping_functions: Vec::<MappingFunction>::new(),
        notification: String::from(""),
        notification_store: String::from(""),
        tags: Vec::<String>::new(),
        thresholds: Vec::<Threshold>::new(),
        trend_store_links: Vec::<TrendStoreLink>::new(),
        weight: String::from(""),
    })
}

pub async fn load_triggers<T: GenericClient + Send + Sync>(
    conn: &mut T,
) -> Result<Vec<Trigger>, Error> {
    let mut triggers: Vec<Trigger> = Vec::new();

    let query = "SELECT name FROM trigger.rule";

    let rows = conn.query(query, &[]).await.map_err(|e| {
        DatabaseError::from_msg(format!("Error loading trend materializations: {}", e))
    })?;

    for row in rows {
        let name = row.get(0);

        let trigger = load_trigger(conn, name).await?;

        triggers.push(trigger);
    }

    return Ok(triggers);
}

pub struct CreateNotifications<Tz: TimeZone> {
    pub trigger_name: String,
    pub timestamp: Option<DateTime<Tz>>,
}

impl<Tz: TimeZone> fmt::Display for CreateNotifications<Tz> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CreateNotifications({})", &self.trigger_name)
    }
}

#[async_trait]
impl<Tz> GenericChange for CreateNotifications<Tz> where Tz: TimeZone, <Tz as TimeZone>::Offset: Sync + Send, DateTime<Tz>: ToSql{
    async fn generic_apply<T: GenericClient + Sync + Send>(&self, client: &mut T) -> ChangeResult {
        let mut transaction = client.transaction().await?;

        let message = create_notifications(&mut transaction, &self.trigger_name, self.timestamp.clone()).await?;

        Ok(message)
    }
}

#[async_trait]
impl<Tz> Change for CreateNotifications<Tz> where Tz: TimeZone + Sync + Send, <Tz as TimeZone>::Offset: Sync + Send, DateTime<Tz>: ToSql {
    async fn apply(&self, client: &mut Client) -> ChangeResult {
        self.generic_apply(client).await
    }
}

pub async fn create_notifications<T: GenericClient + Send + Sync, Ts: ToSql + Send + Sync>(
    conn: &mut T,
    name: &str,
    timestamp: Option<Ts>,
) -> Result<String, Error> where Ts: ToSql {

    let name_string = String::from(name);

    let notification_count: i32 = match timestamp {
        None => {
            let query = String::from("SELECT trigger.create_notifications($1::name)");

            let row = conn
                .query_one(
                    &query,
                    &[&name_string],
                )
                .await
                .map_err(|e| {
                    DatabaseError::from_msg(format!("Error checking for rule existance: {}", e))
                })?;

            row.try_get(0)?
        },
        Some(t) => {
            let query = String::from("SELECT trigger.create_notifications($1::name, $2::timestamptz)");

            let row = conn
                .query_one(
                    &query,
                    &[&name_string, &t],
                )
                .await
                .map_err(|e| {
                    DatabaseError::from_msg(format!("Error checking for rule existance: {}", e))
                })?;

            row.try_get(0)?
        }
    };

    Ok(format!("Created {notification_count} notifications for trigger '{name}'"))
}