use postgres::types::ToSql;
use postgres::Client;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::PathBuf;

type PostgresName = String;

use super::change::Change;
use super::error::{ConfigurationError, DatabaseError, Error, RuntimeError};

#[derive(Debug, Serialize, Deserialize, Clone, ToSql)]
#[postgres(name = "attribute_descr")]
pub struct Attribute {
    pub name: PostgresName,
    pub data_type: String,
    #[serde(default = "default_empty_string")]
    pub description: String,
}

fn default_empty_string() -> String {
    String::new()
}

pub struct AddAttributes {
    pub notification_store: NotificationStore,
    pub attributes: Vec<Attribute>,
}

impl fmt::Display for AddAttributes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AddAttributes({}, {:?})",
            &self.notification_store, &self.attributes
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NotificationStore {
    pub title: Option<String>,
    pub data_source: String,
    pub attributes: Vec<Attribute>,
}

impl NotificationStore {
    pub fn diff(&self, other: &NotificationStore) -> Vec<Box<dyn Change>> {
        let mut changes: Vec<Box<dyn Change>> = Vec::new();

        changes
    }
}

impl fmt::Display for NotificationStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NotificationStore({})", &self.data_source,)
    }
}

pub struct AddNotificationStore {
    pub notification_store: NotificationStore,
}

impl fmt::Display for AddNotificationStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AddNotificationStore({})", &self.notification_store)
    }
}

impl Change for AddNotificationStore {
    fn apply(&self, client: &mut Client) -> Result<String, Error> {
        let query = format!(
            "SELECT notification_directory.create_notification_store($1::text, ARRAY[{}]::notification_directory.attr_def[])",
            self.notification_store.attributes.iter().map(|att| format!("('{}', '{}', '')", &att.name, &att.data_type)).collect::<Vec<String>>().join(",")
        );

        client
            .query_one(&query, &[&self.notification_store.data_source])
            .map_err(|e| {
                DatabaseError::from_msg(format!("Error creating notification store: {}", e))
            })?;

        Ok(format!(
            "Created attribute store '{}'",
            &self.notification_store
        ))
    }
}

pub fn load_notification_stores(conn: &mut Client) -> Result<Vec<NotificationStore>, Error> {
    let mut notification_stores: Vec<NotificationStore> = Vec::new();

    let query = concat!(
        "SELECT notification_store.id, data_source.name ",
        "FROM notification_directory.notification_store ",
        "JOIN directory.data_source ON data_source.id = notification_store.data_source_id ",
    );

    let result = conn.query(query, &[]).map_err(|e| {
        DatabaseError::from_msg(format!("Error loading notification stores: {}", e))
    })?;

    for row in result {
        let attribute_store_id: i32 = row.get(0);
        let data_source: &str = row.get(1);

        let attributes = load_attributes(conn, attribute_store_id);

        notification_stores.push(NotificationStore {
            title: None,
            data_source: String::from(data_source),
            attributes,
        });
    }

    Ok(notification_stores)
}

pub fn load_notification_store(
    conn: &mut Client,
    data_source: &str,
    entity_type: &str,
) -> Result<NotificationStore, Error> {
    let query = concat!(
        "SELECT attribute_store.id ",
        "FROM attribute_directory.attribute_store ",
        "JOIN directory.data_source ON data_source.id = attribute_store.data_source_id ",
        "WHERE data_source.name = $1"
    );

    let result = conn
        .query_one(query, &[&data_source, &entity_type])
        .map_err(|e| DatabaseError::from_msg(format!("Could not load attribute stores: {}", e)))?;

    let attributes = load_attributes(conn, result.get::<usize, i32>(0));

    Ok(NotificationStore {
        title: None,
        data_source: String::from(data_source),
        attributes,
    })
}

fn load_attributes(conn: &mut Client, attribute_store_id: i32) -> Vec<Attribute> {
    let attribute_query = "SELECT name, data_type, description FROM attribute_directory.attribute WHERE attribute_store_id = $1";
    let attribute_result = conn.query(attribute_query, &[&attribute_store_id]).unwrap();

    let mut attributes: Vec<Attribute> = Vec::new();

    for attribute_row in attribute_result {
        let attribute_name: &str = attribute_row.get(0);
        let attribute_data_type: &str = attribute_row.get(1);
        let attribute_description: Option<String> = attribute_row.get(2);

        attributes.push(Attribute {
            name: String::from(attribute_name),
            data_type: String::from(attribute_data_type),
            description: attribute_description.unwrap_or(String::from("")),
        });
    }

    attributes
}

pub fn load_notification_store_from_file(path: &PathBuf) -> Result<NotificationStore, Error> {
    let f = std::fs::File::open(path).map_err(|e| {
        ConfigurationError::from_msg(format!(
            "Could not open notification store definition file '{}': {}",
            path.display(),
            e
        ))
    })?;

    let notification_store: NotificationStore = serde_yaml::from_reader(f).map_err(|e| {
        RuntimeError::from_msg(format!(
            "Could not read notification store definition from file '{}': {}",
            path.display(),
            e
        ))
    })?;

    Ok(notification_store)
}
