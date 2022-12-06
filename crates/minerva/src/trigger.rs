use std::time::Duration;
use std::fmt;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use postgres_protocol::escape::{escape_identifier, escape_literal};
use tokio_postgres::{Client, GenericClient, Row};

use async_trait::async_trait;

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
        write!(
            f,
            "Trigger({})",
            &self.name,
        )
    }
}

pub async fn list_triggers(conn: &mut Client) -> Result<Vec<String>, String> {
    let query = concat!(
        "SELECT name, ns::text, granularity, default_interval, enabled ",
        "FROM trigger.rule ",
        "JOIN notification_directory.notification_store ns ON ns.id = notification_store_id",
    );

    let result = conn.query(query, &[]).await.unwrap();

    let triggers = result
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

    Ok(triggers)
}

pub struct AddTrigger {
    pub trigger: Trigger,
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

        transaction.commit().await?;

        Ok(format!("Created trigger '{}'", &self.trigger.name))
    }
}

async fn create_type<T: GenericClient + Sync + Send>(trigger: &Trigger, client: &mut T) -> ChangeResult {
    let type_name = format!("{}_kpi", &trigger.name);
    let mut cols: Vec<(String, String)> = vec![
        (String::from("entity_id"), String::from("integer")),
        (String::from("timestamp"), String::from("timestamp with time zone")),
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
        .execute(
            &query,
            &[],
        )
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error creating KPI type: {}", e)))?;

    Ok(format!("Added KPI type for trigger '{}'", &trigger.name))
}

async fn create_kpi_function<T: GenericClient + Sync + Send>(trigger: &Trigger, client: &mut T) -> ChangeResult {
    let function_name = format!("{}_kpi", &trigger.name);
    let type_name = format!("{}_kpi", &trigger.name);

    let query = format!(
        "CREATE FUNCTION trigger_rule.{}(timestamp with time zone) RETURNS SETOF trigger_rule.{} AS $trigger${}$trigger$ LANGUAGE plpgsql STABLE;",
        &escape_identifier(&function_name),
        &escape_identifier(&type_name),
        &trigger.kpi_function,
    );

    client
        .execute(
            &query,
            &[],
        )
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error creating KPI function: {}", e)))?;

    Ok(format!("Added KPI function for trigger '{}'", &trigger.name))
}

async fn create_rule<T: GenericClient + Sync + Send>(trigger: &Trigger, client: &mut T) -> ChangeResult {
    let query = format!(
        "SELECT * FROM trigger.create_rule($1, array[{}]::trigger.threshold_def[])",
        trigger.thresholds.iter().map(|threshold| { format!("({}, {})", escape_literal(&threshold.name), escape_literal(&threshold.data_type)) }).collect::<Vec<String>>().join(",")
    );

    client
        .execute(
            &query,
            &[&trigger.name,],
        )
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error creating rule: {}", e)))?;

    Ok(format!("Added rule for trigger '{}'", &trigger.name))
}

async fn set_weight<T: GenericClient + Sync + Send>(trigger: &Trigger, client: &mut T) -> ChangeResult {
    let query = format!(
        "SELECT trigger.set_weight($1::name, $2::text)",
    );

    client
        .execute(
            &query,
            &[&trigger.name, &trigger.weight],
        )
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error setting weight: {}", e)))?;

    Ok(format!("Set weight for trigger '{}'", &trigger.name))
}

async fn set_thresholds<T: GenericClient + Sync + Send>(trigger: &Trigger, client: &mut T) -> ChangeResult {
    let function_name = format!("{}_set_thresholds", &trigger.name);
    let function_args = trigger.thresholds
        .iter()
        .map(|threshold| threshold.value.clone() )
        .collect::<Vec<String>>()
        .join(",");

    let query = format!(
        "SELECT trigger_rule.{}({})",
        &escape_identifier(&function_name),
        function_args,
    );

    client
        .execute(
            &query,
            &[],
        )
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error setting thresholds: {}", e)))?;

    Ok(format!("Set thresholds for trigger '{}'", &trigger.name))
}

async fn set_condition<T: GenericClient + Sync + Send>(trigger: &Trigger, client: &mut T) -> ChangeResult {
    let query = "SELECT trigger.set_condition(rule, $1) FROM trigger.rule WHERE name = $2";

    client
        .execute(
            query,
            &[&trigger.condition, &trigger.name],
        )
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error setting condition: {}", e)))?;

    Ok(format!("Set condition for trigger '{}'", &trigger.name))
}

async fn define_notification_message<T: GenericClient + Sync + Send>(trigger: &Trigger, client: &mut T) -> ChangeResult {
    let query = "SELECT trigger.define_notification_message($1, $2)";

    client
        .execute(
            query,
            &[&trigger.name, &trigger.notification],
        )
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error setting message: {}", e)))?;

    Ok(format!("Set message for trigger '{}'", &trigger.name))
}

async fn define_notification_data<T: GenericClient + Sync + Send>(trigger: &Trigger, client: &mut T) -> ChangeResult {
    let query = "SELECT trigger.define_notification_data($1, $2)";

    client
        .execute(
            query,
            &[&trigger.name, &trigger.data],
        )
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error setting data: {}", e)))?;

    Ok(format!("Set data for trigger '{}'", &trigger.name))
}

async fn create_mapping_functions<T: GenericClient + Sync + Send>(trigger: &Trigger, client: &mut T) -> ChangeResult {
    for mapping_function in trigger.mapping_functions.iter() {
        let query = format!(
            "CREATE FUNCTION trend.{}(timestamp with time zone) RETURNS SETOF timestamp with time zone AS $${}$$ LANGUAGE sql STABLE",
            escape_identifier(&mapping_function.name),
            &mapping_function.source,
        );

        client
            .execute(
                &query,
                &[],
            )
            .await
            .map_err(|e| DatabaseError::from_msg(format!("Error creating mapping function: {}", e)))?;
    }

    Ok(format!("Created mapping functions for trigger '{}'", &trigger.name))
}

async fn link_trend_stores<T: GenericClient + Sync + Send>(trigger: &Trigger, client: &mut T) -> ChangeResult {
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
                &[&mapping_function, &trigger.name, &trend_store_link.part_name],
            )
            .await
            .map_err(|e| DatabaseError::from_msg(format!("Error linking trend store: {}", e)))?;
    }

    Ok(format!("Linked trend stores for trigger '{}'", &trigger.name))
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
        client
            .execute(
                "SELECT trigger.delete_rule($1)",
                &[&self.trigger_name],
            )
            .await
            .map_err(|e| DatabaseError::from_msg(format!("Error deleting rule: {}", e)))?;

        Ok(format!("Rmoved trigger '{}'", &self.trigger_name))
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

pub struct UpdateTriggerData {
    pub trigger: Trigger,
}

impl fmt::Display for UpdateTriggerData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UpdateTriggerData({})", &self.trigger)
    }
}

#[async_trait]
impl GenericChange for UpdateTriggerData {
    async fn generic_apply<T: GenericClient + Sync + Send>(&self, client: &mut T) -> ChangeResult {
        let mut transaction = client.transaction().await?;

        define_notification_data(&self.trigger, &mut transaction).await?;

        transaction.commit().await?;

        Ok(format!("Update data definition of trigger '{}'", &self.trigger.name))
    }
}

#[async_trait]
impl Change for UpdateTriggerData {
    async fn apply(&self, client: &mut Client) -> ChangeResult {
        self.generic_apply(client).await
    }
}