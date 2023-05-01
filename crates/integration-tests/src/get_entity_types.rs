#[cfg(test)]
mod tests {
    use assert_cmd::prelude::*;
    use std::process::Command;

    use rand::distributions::{Alphanumeric, DistString}; 

    use minerva::change::Change;
    use minerva::database::{connect_to_db, get_db_config, create_database, drop_database};

    use minerva::schema::create_schema;
    use minerva::trend_store::{TrendStore, AddTrendStore, create_partitions_for_timestamp};

    const TREND_STORE_DEFINITION: &str = r###"
    title: Raw node data
    data_source: hub
    entity_type: node
    granularity: 15m
    partition_size: 1d
    parts:
      - name: hub_node_main_15m
        trends:
          - name: outside_temp
            data_type: numeric
          - name: inside_temp
            data_type: numeric
          - name: power_kwh
            data_type: numeric
          - name: freq_power
            data_type: numeric
        generated_trends:
          - name: power_Mwh
            data_type: numeric
            description: test
            expression: power_kwh / 1000

    "###;

    fn generate_name() -> String {
         Alphanumeric.sample_string(&mut rand::thread_rng(), 16)
    }

    #[cfg(test)]
    #[tokio::test]
    async fn get_entity_types() -> Result<(), Box<dyn std::error::Error>> {
        let database_name = generate_name();
        let db_config = get_db_config()?;
        let mut client = connect_to_db(&db_config).await?;

        create_database(&mut client, &database_name).await?;
        println!("Created database '{database_name}'");

        {
            let mut client = connect_to_db(&db_config.clone().dbname(&database_name)).await?;
            create_schema(&mut client).await?;

            let trend_store: TrendStore = serde_yaml::from_str(TREND_STORE_DEFINITION).map_err(|e| {
                format!("Could not read trend store definition: {}", e)
            })?;

            let add_trend_store = AddTrendStore { trend_store };

            add_trend_store.apply(&mut client).await?;
            let timestamp = chrono::DateTime::parse_from_rfc3339("2023-03-25T14:00:00+00:00").unwrap();
            create_partitions_for_timestamp(&mut client, timestamp).await?;
        }

        let mut cmd = Command::cargo_bin("minerva-service")?;
        cmd.env("PGDATABASE", &database_name);

        let mut proc_handle = cmd.spawn().expect("Process started");

        println!("Started service");

        let body = reqwest::get("http://localhost:8000/entity-types")
            .await?
            .text()
            .await?;

        match proc_handle.kill() {
            Err(e) => println!("Could not stop web service: {e}"),
            Ok(_) => (),
        }

        let mut client = connect_to_db(&db_config).await?;

        drop_database(&mut client, &database_name).await?;

        println!("Dropped database '{database_name}'");

        assert_eq!(body, "[{\"id\":1,\"name\":\"node\",\"description\":\"\"}]");

        Ok(())
    }
}
