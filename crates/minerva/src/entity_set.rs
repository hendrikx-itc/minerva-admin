use std::fmt;

use serde::{Deserialize, Serialize};

use chrono::{DateTime, Utc};
use tokio_postgres::Client;

type PostgresName = String;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EntitySet {
    pub name: PostgresName,
    pub group: String,
    pub entity_type: String,
    pub owner: String,
    pub description: String,
    pub entities: Vec<String>,
    pub created: DateTime<Utc>,
    pub modified: DateTime<Utc>,
}

impl fmt::Display for EntitySet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EntitySet({}:{})", &self.owner, &self.name,)
    }
}

pub async fn load_entity_sets(
    conn: &mut Client,
) -> Result<Vec<EntitySet>, String> {
    let query = concat!(
        "SELECT es.name, es.group, et.name, es.owner, es.description, ",
        "entity_set.get_entity_set_members(es.owner, es.name), es.created, es.modified ",
        "FROM directory.entity_set es JOIN directory.entity_type et ON es.entity_type_id = et.id"
    );

    let rows = conn.query(query, &[])
        .await
        .map_err(|e| format!("Error loading entity sets: {e}"))?;

    let entity_sets = rows
        .iter()
        .map(|row| EntitySet {
            name: row.get(0),
            group: row.get(1),
            entity_type: row.get(2),
            owner: row.get(3),
            description: row.try_get(4).unwrap_or("".into()),
            entities: row.get(5),
            created: row.get(6),
            modified: row.get(7),
        })
        .collect();
     
    Ok(entity_sets)
}

pub async fn load_entity_set(
    conn: &mut Client,
    owner: &str,
    name: &str
) -> Result<EntitySet, String> {
    let query = concat!(
        "SELECT es.name, es.group, et.name, es.owner, es.description, ",
        "entity_set.get_entity_set_members(es.owner, es.name), es.created, es.modified ",
        "FROM directory.entity_set es JOIN directory.entity_type et ON es.entity_type_id = et.id ",
        "WHERE es.owner = $1 AND es.name = $2"
    );

    let row = conn.query_one(query, &[&owner, &name]).await.map_err(|e| {
        format!("Could not load entity set {owner}:{name}: {e}")
    })?;

    let entity_set = EntitySet {
            name: row.get(0),
            group: row.get(1),
            entity_type: row.get(2),
            owner: row.get(3),
            description: row.try_get(4).unwrap_or("".into()),
            entities: row.get(5),
            created: row.get(6),
            modified: row.get(7),
    };

    Ok(entity_set)
}