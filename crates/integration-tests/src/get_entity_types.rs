#[cfg(test)]
mod tests {
    use assert_cmd::prelude::*;
    use std::process::Command;
    use std::time::Duration;
    use std::net::{TcpStream, SocketAddr};

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

        let service_address = "127.0.0.1";
        let service_port = "8010";

        let mut cmd = Command::cargo_bin("minerva-service")?;
        cmd
            .env("PGDATABASE", &database_name)
            .env("SERVICE_ADDRESS", service_address)
            .env("SERVICE_PORT", service_port);

        let mut proc_handle = cmd.spawn().expect("Process started");

        println!("Started service");

        let address = format!("{service_address}:{service_port}");

        let url = format!("http://{address}/entity-types");
        let timeout = Duration::from_millis(1000);

        let ipv4_addr: SocketAddr = address.parse().unwrap();

        loop {
            let result = TcpStream::connect_timeout(&ipv4_addr, timeout);

            match result {
                Ok(_) => break,
                Err(_) => tokio::time::sleep(timeout).await
            }
        }

        //let response = reqwest::get(url).await;
        //let body = reqwest::get(url)
        //    .await?
        //    .text()
        //    .await?;

        match proc_handle.kill() {
            Err(e) => println!("Could not stop web service: {e}"),
            Ok(_) => println!("Stopped web service"),
        }

        let mut client = connect_to_db(&db_config).await?;

        drop_database(&mut client, &database_name).await?;

        println!("Dropped database '{database_name}'");

        //assert_eq!(body, "[{\"id\":1,\"name\":\"node\",\"description\":\"\"}]");

        Ok(())
    }
}
