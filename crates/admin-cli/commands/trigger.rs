use std::path::PathBuf;

use async_trait::async_trait;
use structopt::StructOpt;

use comfy_table::presets::UTF8_HORIZONTAL_ONLY;
use comfy_table::Table;

use minerva::change::Change;
use minerva::error::DatabaseError;
use minerva::trigger::{
    list_triggers, load_trigger_from_file, AddTrigger, DeleteTrigger, UpdateTrigger, VerifyTrigger
};

use super::common::{connect_db, Cmd, CmdResult};

#[derive(Debug, StructOpt)]
pub struct TriggerList {}

#[async_trait]
impl Cmd for TriggerList {
    async fn run(&self) -> CmdResult {
        let mut client = connect_db().await?;

        let triggers = list_triggers(&mut client)
            .await
            .map_err(|e| DatabaseError::from_msg(format!("Error listing triggers: {}", e)))?;

        let mut table = Table::new();
        table.load_preset(UTF8_HORIZONTAL_ONLY);
        table.set_header(vec![
            "Name",
            "Notification Store",
            "Granularity",
            "Default Interval",
        ]);
        for trigger in triggers {
            table.add_row(vec![trigger.0, trigger.1, trigger.2, trigger.3]);
        }

        println!("{table}");

        Ok(())
    }
}

#[derive(Debug, StructOpt)]
pub struct TriggerCreate {
    #[structopt(
        short = "-v",
        long = "--verify",
        help = "run basic verification commands after update"
    )]
    verify: bool,
    #[structopt(help = "trigger definition file")]
    definition: PathBuf,
}

#[async_trait]
impl Cmd for TriggerCreate {
    async fn run(&self) -> CmdResult {
        let trigger = load_trigger_from_file(&self.definition)?;

        println!("Loaded definition, creating trigger");

        let mut client = connect_db().await?;

        let change = AddTrigger { trigger, verify: self.verify };

        let message = change.apply(&mut client).await?;

        println!("{message}");

        Ok(())
    }
}

#[derive(Debug, StructOpt)]
pub struct TriggerDelete {
    #[structopt(help = "trigger name")]
    name: String,
}

#[async_trait]
impl Cmd for TriggerDelete {
    async fn run(&self) -> CmdResult {
        let mut client = connect_db().await?;

        let change = DeleteTrigger {
            trigger_name: self.name.clone(),
        };

        change.apply(&mut client).await?;

        println!("Deleted trigger '{}'", &self.name);

        Ok(())
    }
}

#[derive(Debug, StructOpt)]
pub struct TriggerUpdate {
    #[structopt(
        short = "-v",
        long = "--verify",
        help = "run basic verification commands after update"
    )]
    verify: bool,
    #[structopt(help = "trigger definition file")]
    definition: PathBuf,
}

#[async_trait]
impl Cmd for TriggerUpdate {
    async fn run(&self) -> CmdResult {
        let trigger = load_trigger_from_file(&self.definition)?;

        let mut client = connect_db().await?;

        let change = UpdateTrigger {
            trigger,
            verify: self.verify,
        };

        let message = change.apply(&mut client).await?;

        println!("{message}");

        Ok(())
    }
}

#[derive(Debug, StructOpt)]
pub struct TriggerVerify {
    #[structopt(help = "trigger name")]
    name: String,
}

#[async_trait]
impl Cmd for TriggerVerify {
    async fn run(&self) -> CmdResult {
        let mut client = connect_db().await?;

        let change = VerifyTrigger {
            trigger_name: self.name.clone(),
        };

        let message = change.apply(&mut client).await?;

        println!("{message}");

        Ok(())
    }
}

#[derive(Debug, StructOpt)]
pub enum TriggerOpt {
    #[structopt(about = "list configured triggers")]
    List(TriggerList),
    #[structopt(about = "create a trigger")]
    Create(TriggerCreate),
    #[structopt(about = "delete a trigger")]
    Delete(TriggerDelete),
    #[structopt(about = "update a trigger")]
    Update(TriggerUpdate),
    #[structopt(about = "run basic verification on a trigger")]
    Verify(TriggerVerify),
}

impl TriggerOpt {
    pub async fn run(&self) -> CmdResult {
        match self {
            TriggerOpt::List(list) => list.run().await,
            TriggerOpt::Create(create) => create.run().await,
            TriggerOpt::Delete(delete) => delete.run().await,
            TriggerOpt::Update(update) => update.run().await,
            TriggerOpt::Verify(verify) => verify.run().await,
        }
    }
}
