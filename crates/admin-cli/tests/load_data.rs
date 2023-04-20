use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::process::Command;
use std::path::PathBuf;

#[test]
#[ignore]
fn load_data() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("minerva-admin")?;

    let instance_root_path = std::fs::canonicalize("../../examples/tiny_instance_v1").unwrap();

    let mut file_path = PathBuf::from(instance_root_path);
    file_path.push("sample-data/sample.csv");

    cmd.arg("load-data").arg(&file_path);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Created trigger"));

    Ok(())
}
