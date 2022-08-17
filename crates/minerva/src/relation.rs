use std::fmt;
use std::path::PathBuf;
use std::future::Future;
use std::pin::Pin;

use tokio_postgres::Client;
use serde::{Deserialize, Serialize};

use super::change::Change;
use super::error::{ConfigurationError, DatabaseError, Error, RuntimeError};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Relation {
    pub name: String,
    pub query: String,
}

impl fmt::Display for Relation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Relation({})", &self.name)
    }
}

pub fn load_relation_from_file(path: &PathBuf) -> Result<Relation, Error> {
    let f = std::fs::File::open(path).map_err(|e| {
        ConfigurationError::from_msg(format!(
            "Could not open relation definition file '{}': {}",
            path.display(),
            e
        ))
    })?;

    if path.extension() == Some(std::ffi::OsStr::new("yaml")) {
        let relation: Relation = serde_yaml::from_reader(f).map_err(|e| {
            RuntimeError::from_msg(format!(
                "Could not read relation definition from file '{}': {}",
                path.display(),
                e
            ))
        })?;

        Ok(relation)
    } else if path.extension() == Some(std::ffi::OsStr::new("json")) {
        let relation: Relation = serde_json::from_reader(f).map_err(|e| {
            RuntimeError::from_msg(format!(
                "Could not read relation definition from file '{}': {}",
                path.display(),
                e
            ))
        })?;

        Ok(relation)
    } else {
        return Err(ConfigurationError::from_msg(format!(
            "Unsupported relation definition format '{}'",
            path.extension().unwrap().to_string_lossy()
        ))
        .into());
    }
}

pub struct AddRelation {
    pub relation: Relation,
}

impl fmt::Display for AddRelation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AddRelation({})", &self.relation)
    }
}

impl Change for AddRelation {
    type ChangeResultType = Pin<Box<dyn Future<Output = Result<String, Error>>>>;

    fn apply(&self, client: &mut Client) -> Pin<Box<dyn Future<Output = Result<String, Error>>>> {
        Box::pin(apply_add_relation(self, client))
    }
}

async fn apply_add_relation(add_relation: &AddRelation, client: &mut Client) -> Result<String, Error> {
    let query = format!(
        "CREATE MATERIALIZED VIEW relation.\"{}\" AS {}",
        add_relation.relation.name, add_relation.relation.query
    );

    client.query(&query, &[]).await.map_err(|e| {
        DatabaseError::from_msg(format!("Error creating relation materialized view: {}", e))
    })?;

    let query = "SELECT relation_directory.register_type($1)";

    client
        .query_one(query, &[&add_relation.relation.name])
        .await
        .map_err(|e| DatabaseError::from_msg(format!("Error registering relation: {}", e)))?;

    Ok(format!("Added relation {}", &add_relation.relation))
}

impl From<Relation> for AddRelation {
    fn from(relation: Relation) -> Self {
        AddRelation { relation }
    }
}
