#[cfg(test)]
mod tests {
    use std::process::Command;

    use assert_cmd::prelude::*;
    use predicates::prelude::*;

    use minerva::database::{connect_db, create_database, drop_database};

    #[tokio::test]
    async fn initialize() -> Result<(), Box<dyn std::error::Error>> {
        let database_name = "minerva";
        let mut client = connect_db().await?;

        drop_database(&mut client, database_name).await?;
        create_database(&mut client, database_name).await?;

        println!("Dropped database");
        let mut cmd = Command::cargo_bin("minerva-admin")?;
        cmd.env("PGDATABASE", database_name);

        let instance_root_path = std::fs::canonicalize("../../examples/tiny_instance_v1").unwrap();

        cmd.arg("initialize").arg("--create-schema").arg("--with-definition").arg(&instance_root_path);
        cmd.assert()
            .success()
            .stdout(predicate::str::contains("Created trigger"));

        let mut client = connect_db().await?;

        drop_database(&mut client, database_name).await?;

        println!("Dropped database");

        Ok(())
    }
}
