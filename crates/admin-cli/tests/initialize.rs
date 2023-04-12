use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::process::Command;

#[test]
#[ignore]
fn initialize() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("minerva-admin")?;

    let instance_root_path = std::fs::canonicalize("../../examples/tiny_instance_v1").unwrap();

    cmd.arg("initialize").arg(&instance_root_path);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Created trigger"));

    Ok(())
}
