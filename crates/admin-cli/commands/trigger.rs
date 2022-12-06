use std::path::PathBuf;

use structopt::StructOpt;
use async_trait::async_trait;

use minerva::change::Change;
use minerva::trigger::{list_triggers, AddTrigger, DeleteTrigger, load_trigger_from_file};

use super::common::{Cmd, CmdResult, connect_db};

#[derive(Debug, StructOpt)]
pub struct TriggerList {}

#[async_trait]
impl Cmd for TriggerList {
    async fn run(&self) -> CmdResult {
        let mut client = connect_db().await?;

        let triggers = list_triggers(&mut client).await.unwrap();
    
        for trigger in triggers {
            println!("{}", &trigger);
        }
    
        Ok(())
    }
}

#[derive(Debug, StructOpt)]
pub struct TriggerCreate {
    #[structopt(help = "trigger definition file")]
    definition: PathBuf,
}

#[async_trait]
impl Cmd for TriggerCreate {
    async fn run(&self) -> CmdResult {
        let trigger = load_trigger_from_file(&self.definition)?;
        let trigger_name = trigger.name.clone();

        println!("Loaded definition, creating trigger");

        let mut client = connect_db().await?;

        let change = AddTrigger { trigger };

        change.apply(&mut client).await?;

        println!("Created trigger '{}'", &trigger_name);

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

        let change = DeleteTrigger { trigger_name: self.name.clone(), };

        change.apply(&mut client).await?;

        println!("Deleted trigger '{}'", &self.name);

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
}

impl TriggerOpt {
    pub async fn run(&self) -> CmdResult {
        match self {
            TriggerOpt::List(list) => list.run().await,
            TriggerOpt::Create(create) => create.run().await,
            TriggerOpt::Delete(delete) => delete.run().await,
        }
    }
}