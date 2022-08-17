use std::fmt;
use std::{io::Read, path::PathBuf};
use std::future::Future;
use std::pin::Pin;
use std::boxed::Box;

use tokio_postgres::Client;
use serde::{Deserialize, Serialize};

use super::change::Change;
use super::error::{ConfigurationError, DatabaseError, Error};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct VirtualEntity {
    pub name: String,
    pub sql: String,
}

impl fmt::Display for VirtualEntity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VirtualEntity({})", &self.name)
    }
}

pub fn load_virtual_entity_from_file(path: &PathBuf) -> Result<VirtualEntity, Error> {
    let mut f = std::fs::File::open(path).map_err(|e| {
        ConfigurationError::from_msg(format!(
            "Could not open relation definition file '{}': {}",
            path.display(),
            e
        ))
    })?;

    let mut sql = String::new();

    f.read_to_string(&mut sql).map_err(|e| {
        ConfigurationError::from_msg(format!(
            "Could not read virtual entity definition file: {}",
            e
        ))
    })?;

    let name = path.file_name().unwrap().to_string_lossy().to_string();

    let virtual_entity = VirtualEntity { name, sql };

    Ok(virtual_entity)
}

pub struct AddVirtualEntity {
    pub virtual_entity: VirtualEntity,
}

impl fmt::Display for AddVirtualEntity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AddVirtualEntity({})", &self.virtual_entity)
    }
}

impl Change for AddVirtualEntity {
    type ChangeResultType = Pin<Box<dyn Future<Output = Result<String, Error>>>>;

    fn apply(&self, client: &mut Client) -> Self::ChangeResultType {
        Box::pin(apply_add_virtual_entity(self, client))
    }
}

async fn apply_add_virtual_entity(add_virtual_entity: &AddVirtualEntity, client: &mut Client) -> Result<String, Error> {
    client
        .batch_execute(&add_virtual_entity.virtual_entity.sql)
        .await
        .map_err(|e| {
            DatabaseError::from_msg(format!("Error creating relation materialized view: {}", e))
        })?;

    Ok(format!("Added virtual entity {}", &add_virtual_entity.virtual_entity))
}

impl From<VirtualEntity> for AddVirtualEntity {
    fn from(virtual_entity: VirtualEntity) -> Self {
        AddVirtualEntity { virtual_entity }
    }
}
