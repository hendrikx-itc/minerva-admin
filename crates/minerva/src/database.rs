
use tokio_postgres::Client;

pub async fn create_database<'a>(client: &'a mut Client, database_name: &str) -> Result<(), String> {
    let query = format!("CREATE DATABASE \"{database_name}\"");

    client.execute(&query, &[]).await.map_err(|e| {
        format!("Error creating database '{database_name}': {e}")
    })?;

    Ok(())
}

pub async fn drop_database<'a>(client: &'a mut Client, database_name: &str) -> Result<(), String> {
    let query = format!("DROP DATABASE IF EXISTS \"{database_name}\"");

    client.execute(&query, &[]).await.map_err(|e| {
        format!("Error dropping database '{database_name}': {e}")
    })?;

    Ok(())
}
