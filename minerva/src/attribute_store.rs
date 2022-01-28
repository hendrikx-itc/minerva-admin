use postgres::types::ToSql;
use postgres::Client;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::PathBuf;

type PostgresName = String;

use super::change::Change;
use super::error::{Error, DatabaseError, ConfigurationError, RuntimeError};

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
    pub attribute_store: AttributeStore,
    pub attributes: Vec<Attribute>,
}

impl fmt::Display for AddAttributes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AddAttributes({}, {:?})",
            &self.attribute_store, &self.attributes
        )
    }
}

impl Change for AddAttributes {
    fn apply(&self, client: &mut Client) -> Result<String, Error> {
        let query = concat!(
            "SELECT attribute_directory.add_attributes(attribute_store, $1) ",
            "FROM attribute_directory.attribute_store ",
            "JOIN directory.data_source ON data_source.id = attribute_store.data_source_id ",
            "JOIN directory.entity_type ON entity_type.id = attribute_store.entity_type_id ",
            "WHERE data_source.name = $2 AND entity_type.name = $3",
        );

        client.query_one(
            query,
            &[
                &self.attributes,
                &self.attribute_store.data_source,
                &self.attribute_store.entity_type,
            ],
        ).map_err(|e| {
            DatabaseError::from_msg(format!("Error adding trends to trend store part: {}", e))
        })?;

        Ok(format!("Added attributes to attribute store '{}'", &self.attribute_store))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AttributeStore {
    pub data_source: String,
    pub entity_type: String,
    pub attributes: Vec<Attribute>,
}

impl AttributeStore {
    pub fn diff(&self, other: &AttributeStore) -> Vec<Box<dyn Change>> {
        let mut changes: Vec<Box<dyn Change>> = Vec::new();

        let mut new_attributes: Vec<Attribute> = Vec::new();

        for other_attribute in &other.attributes {
            match self
                .attributes
                .iter()
                .find(|my_part| my_part.name == other_attribute.name)
            {
                Some(_my_part) => {
                    // Ok attribute exists
                }
                None => {
                    new_attributes.push(other_attribute.clone());
                }
            }
        }

        if new_attributes.len() > 0 {
            changes.push(Box::new(AddAttributes {
                attribute_store: self.clone(),
                attributes: new_attributes,
            }));
        }

        changes
    }
}

impl fmt::Display for AttributeStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AttributeStore({}, {})",
            &self.data_source, &self.entity_type
        )
    }
}

pub struct AddAttributeStore {
    pub attribute_store: AttributeStore,
}

impl fmt::Display for AddAttributeStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AddAttributeStore({})", &self.attribute_store)
    }
}

impl Change for AddAttributeStore {
    fn apply(&self, client: &mut Client) -> Result<String, Error> {
        let query = concat!(
            "SELECT id ",
            "FROM attribute_directory.create_attribute_store(",
            "$1::text, $2::text, ",
            "$3::attribute_directory.attribute_descr[]",
            ")"
        );

        client.query_one(
            query,
            &[
                &self.attribute_store.data_source,
                &self.attribute_store.entity_type,
                &self.attribute_store.attributes,
            ],
        ).map_err(|e|{
            DatabaseError::from_msg(format!("Error creating attribute store: {}", e))
        })?;

        Ok(format!("Created attribute store '{}'", &self.attribute_store))
    }
}

pub fn load_attribute_stores(conn: &mut Client) -> Result<Vec<AttributeStore>, Error> {
    let mut attribute_stores: Vec<AttributeStore> = Vec::new();

    let query = concat!(
        "SELECT attribute_store.id, data_source.name, entity_type.name ",
        "FROM attribute_directory.attribute_store ",
        "JOIN directory.data_source ON data_source.id = attribute_store.data_source_id ",
        "JOIN directory.entity_type ON entity_type.id = attribute_store.entity_type_id"
    );

    let result = conn.query(query, &[]).map_err(|e|{
        DatabaseError::from_msg(format!("Error loading attribute stores: {}", e))
    })?;

    for row in result {
        let attribute_store_id: i32 = row.get(0);
        let data_source: &str = row.get(1);
        let entity_type: &str = row.get(2);

        let attributes = load_attributes(conn, attribute_store_id);

        attribute_stores.push(AttributeStore {
            data_source: String::from(data_source),
            entity_type: String::from(entity_type),
            attributes: attributes,
        });
    }

    Ok(attribute_stores)
}

pub fn load_attribute_store(
    conn: &mut Client,
    data_source: &str,
    entity_type: &str,
) -> Result<AttributeStore, Error> {
    let query = concat!(
        "SELECT attribute_store.id ",
        "FROM attribute_directory.attribute_store ",
        "JOIN directory.data_source ON data_source.id = attribute_store.data_source_id ",
        "JOIN directory.entity_type ON entity_type.id = attribute_store.entity_type_id ",
        "WHERE data_source.name = $1 AND entity_type.name = $2"
    );

    let result = conn
        .query_one(query, &[&data_source, &entity_type])
        .map_err(|e| {
            DatabaseError::from_msg(format!("Could not load attribute stores: {}", e))
        })?;

    let attributes = load_attributes(conn, result.get::<usize, i32>(0));

    Ok(AttributeStore {
        data_source: String::from(data_source),
        entity_type: String::from(entity_type),
        attributes: attributes,
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

pub fn load_attribute_store_from_file(path: &PathBuf) -> Result<AttributeStore, Error> {
    let f = std::fs::File::open(path).map_err(|e| {
        ConfigurationError::from_msg(format!("Could not open attribute store definition file '{}': {}", path.display(), e))
    })?;
    
    let trend_store: AttributeStore = serde_yaml::from_reader(f).map_err(|e| {
        RuntimeError::from_msg(format!("Could not read trend store definition from file '{}': {}", path.display(), e))
    })?;

    Ok(trend_store)
}
