use serde_json::Value;
use tokio_postgres::GenericClient;

pub async fn start_job<T: GenericClient + Send + Sync>(
    client: &mut T,
    description: &Value,
) -> Result<i64, String> {
    let query = "SELECT logging.start_job($1)";

    let result = client
        .query_one(query, &[&description])
        .await
        .map_err(|e| format!("Error starting job: {e}"))?;

    let job_id = result.get(0);

    Ok(job_id)
}

pub async fn end_job<T: GenericClient + Send + Sync>(
    client: &mut T,
    job_id: i64,
) -> Result<(), String> {
    let query = "SELECT logging.end_job($1)";

    client
        .execute(query, &[&job_id])
        .await
        .map_err(|e| format!("Error ending job: {e}"))?;

    Ok(())
}
