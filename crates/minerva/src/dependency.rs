use async_trait::async_trait;
use std::fmt;

use postgres_protocol::escape::escape_identifier;
use tokio_postgres::Client;

use super::change::{ChangeResult, ChangeStep};
use super::error::{DatabaseError, Error};

#[async_trait]
pub trait Dependee: fmt::Display + Send + Sync {
    fn name(&self) -> &str;
    async fn drop_object(&self, client: &mut Client) -> ChangeResult;
}

struct ViewDependee {
    pub schema: String,
    pub name: String,
}

#[async_trait]
impl Dependee for ViewDependee {
    fn name(&self) -> &str {
        &self.name
    }

    async fn drop_object(&self, client: &mut Client) -> ChangeResult {
        let query = format!(
            "DROP VIEW IF EXISTS {}.{}",
            &escape_identifier(&self.schema),
            &escape_identifier(&self.name),
        );

        match client.execute(query.as_str(), &[]).await {
            Ok(_) => Ok(format!("Dropped view '{}.{}'", &self.schema, &self.name)),
            Err(e) => Err(Error::Database(DatabaseError::from_msg(format!(
                "Error dropping view: {}",
                e
            )))),
        }
    }
}

impl fmt::Display for ViewDependee {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "View('{}.{}')", &self.schema, &self.name,)
    }
}

pub async fn get_column_dependees(
    client: &mut Client,
    schema: &str,
    table_name: &str,
    column_name: &str,
) -> Result<Vec<Box<dyn Dependee>>, Error> {
    let mut dependees: Vec<Box<dyn Dependee>> = Vec::new();

    let query = concat!(
        "select v.relname ",
        "from pg_namespace n ",
        "join pg_class c ON c.relnamespace = n.oid ",
        "join pg_depend dep on dep.refobjid = c.oid ",
        "join pg_attribute attr on attr.attrelid = c.oid and attr.attnum = dep.refobjsubid ",
        "join pg_rewrite rwr on dep.objid = rwr.oid ",
        "join pg_class v on v.oid = rwr.ev_class and v.relkind = 'v' ",
        "where n.nspname = $1 AND c.relname = $2 AND attr.attname = $3"
    );

    let rows = client
        .query(query, &[&schema, &table_name, &column_name])
        .await?;

    for row in rows {
        dependees.push(Box::new(ViewDependee {
            schema: "trend".into(),
            name: row.get(0),
        }))
    }

    Ok(dependees)
}

pub struct DropDependee {
    pub dependee: Box<dyn Dependee>,
}

impl fmt::Display for DropDependee {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DropViewDependee('{}')",
            &self.dependee.name(),
        )
    }
}

#[async_trait]
impl ChangeStep for DropDependee {
    async fn apply(&self, client: &mut Client) -> ChangeResult {
        self.dependee.drop_object(client).await
    }
}
