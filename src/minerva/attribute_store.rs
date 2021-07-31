use postgres::types::ToSql;
use postgres::Client;
use serde::{Deserialize, Serialize};

type PostgresName = String;

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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AttributeStore {
    pub data_source: String,
    pub entity_type: String,
    pub attributes: Vec<Attribute>,
}

pub fn load_attribute_stores(conn: &mut Client) -> Vec<AttributeStore> {
    let mut attribute_stores: Vec<AttributeStore> = Vec::new();

    let query = concat!(
        "SELECT attribute_store.id, data_source.name, entity_type.name ",
        "FROM attribute_directory.attribute_store ",
        "JOIN directory.data_source ON data_source.id = attribute_store.data_source_id ",
        "JOIN directory.entity_type ON entity_type.id = attribute_store.entity_type_id"
    );

    let result = conn.query(query, &[]).unwrap();

    for row in result {
        let attribute_query = "SELECT name, data_type, description FROM attribute_directory.attribute WHERE attribute_store_id = $1";

        let attribute_store_id: i32 = row.get(0);
        let data_source: &str = row.get(1);
        let entity_type: &str = row.get(2);
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

        attribute_stores.push(AttributeStore {
            data_source: String::from(data_source),
            entity_type: String::from(entity_type),
            attributes: attributes,
        });
    }

    attribute_stores
}

pub fn create_attribute_store(conn: &mut Client, attribute_store: &AttributeStore) -> Option<i32> {
    let query = concat!(
        "SELECT id ",
        "FROM attribute_directory.create_attribute_store(",
        "$1::text, $2::text, ",
        "$3::attribute_directory.attribute_descr[]",
        ")"
    );

    let result = conn.query_one(
        query,
        &[
            &attribute_store.data_source,
            &attribute_store.entity_type,
            &attribute_store.attributes,
        ],
    );

    match result {
        Ok(row) => {
            let value: i32 = row.get(0);

            Some(value)
        }
        Err(e) => {
            println!("Error creating attribute store: {}", e);
            None
        }
    }
}
