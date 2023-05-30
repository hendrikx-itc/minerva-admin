#[cfg(test)]
mod tests {
    use assert_cmd::prelude::*;
    use predicates::prelude::*;
    use std::io::Write;
    use std::path::PathBuf;
    use std::process::Command;

    use rand::distributions::{Alphanumeric, DistString};

    use minerva::change::Change;
    use minerva::database::{connect_to_db, create_database, drop_database, get_db_config};

    use minerva::schema::create_schema;
    use minerva::trend_store::{create_partitions_for_timestamp, AddTrendStore, TrendStore};

    const TEST_CSV_DATA: &str = r###"
node,timestamp,outside_temp,inside_temp,power_kwh,freq_power
hillside14,2023-03-25T14:00:00Z,14.4,32.4,55.8,212.4
hillside15,2023-03-25T14:00:00Z,14.5,32.5,55.9,212.5
    "###;

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
    async fn load_data() -> Result<(), Box<dyn std::error::Error>> {
        let data_source_name = "hub";
        let database_name = generate_name();
        let db_config = get_db_config()?;
        let mut client = connect_to_db(&db_config).await?;

        create_database(&mut client, &database_name).await?;
        println!("Created database '{database_name}'");

        {
            let mut client = connect_to_db(&db_config.clone().dbname(&database_name)).await?;
            create_schema(&mut client).await?;

            let trend_store: TrendStore = serde_yaml::from_str(TREND_STORE_DEFINITION)
                .map_err(|e| format!("Could not read trend store definition: {}", e))?;

            let add_trend_store = AddTrendStore { trend_store };

            add_trend_store.apply(&mut client).await?;
            let timestamp =
                chrono::DateTime::parse_from_rfc3339("2023-03-25T14:00:00+00:00").unwrap();
            create_partitions_for_timestamp(&mut client, timestamp.into()).await?;
        }

        let mut cmd = Command::cargo_bin("minerva-admin")?;
        cmd.env("PGDATABASE", &database_name);

        let mut csv_file = tempfile::tempfile().unwrap();

        csv_file.write_all(TEST_CSV_DATA.as_bytes()).unwrap();

        let instance_root_path = std::fs::canonicalize("../../examples/tiny_instance_v1").unwrap();

        let mut file_path = PathBuf::from(instance_root_path);
        file_path.push("sample-data/sample.csv");

        cmd.arg("load-data")
            .arg("--data-source")
            .arg(&data_source_name)
            .arg(&file_path);
        cmd.assert()
            .success()
            .stdout(predicate::str::contains("Job ID"));

        let mut client = connect_to_db(&db_config).await?;

        drop_database(&mut client, &database_name).await?;

        println!("Dropped database '{database_name}'");

        Ok(())
    }
}
