use tokio_postgres::Client;

pub async fn create_schema<'a>(client: &'a mut Client) -> Result<(), String> {
    let sql = include_str!("schema.sql");

    if let Err(e) = client.batch_execute(&sql).await {
        return Err(format!("Error creating Minerva schema: {e}"));
    }

    Ok(())
}
