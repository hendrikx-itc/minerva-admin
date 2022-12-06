use std::path::PathBuf;

use structopt::StructOpt;

use minerva::change::Change;
use minerva::error::{Error, RuntimeError};
use minerva::attribute_store::{
    load_attribute_store, load_attribute_store_from_file, AddAttributeStore, AttributeStore,
};

use super::common::{CmdResult, connect_db};

#[derive(Debug, StructOpt)]
pub struct AttributeStoreCreate {
    #[structopt(help = "attribute store definition file")]
    definition: PathBuf,
}

#[derive(Debug, StructOpt)]
pub struct AttributeStoreUpdate {
    #[structopt(help = "attribute store definition file")]
    definition: PathBuf,
}

#[derive(Debug, StructOpt)]
pub enum AttributeStoreOpt {
    #[structopt(about = "create an attribute store")]
    Create(AttributeStoreCreate),
    #[structopt(about = "update an attribute store")]
    Update(AttributeStoreUpdate),
}

impl AttributeStoreOpt {
    pub async fn run(&self) -> CmdResult {
        match self {
            AttributeStoreOpt::Create(args) => run_attribute_store_create_cmd(&args).await,
            AttributeStoreOpt::Update(args) => run_attribute_store_update_cmd(&args).await,
        }
    }
}

async fn run_attribute_store_create_cmd(args: &AttributeStoreCreate) -> CmdResult {
    let attribute_store: AttributeStore = load_attribute_store_from_file(&args.definition)?;

    println!("Loaded definition, creating attribute store");

    let mut client = connect_db().await?;

    let change = AddAttributeStore { attribute_store };

    let result = change.apply(&mut client).await;

    match result {
        Ok(_) => {
            println!("Created attribute store");

            Ok(())
        }
        Err(e) => Err(Error::Runtime(RuntimeError {
            msg: format!("Error creating attribute store: {}", e),
        })),
    }
}

async fn run_attribute_store_update_cmd(args: &AttributeStoreUpdate) -> CmdResult {
    let attribute_store: AttributeStore = load_attribute_store_from_file(&args.definition)?;

    println!("Loaded definition, updating attribute store");

    let mut client = connect_db().await?;

    let attribute_store_db = load_attribute_store(
        &mut client,
        &attribute_store.data_source,
        &attribute_store.entity_type,
    )
    .await?;

    let changes = attribute_store_db.diff(&attribute_store);

    if !changes.is_empty() {
        println!("Updating attribute store");

        for change in changes {
            let apply_result = change.apply(&mut client).await;

            match apply_result {
                Ok(_) => {
                    println!("{}", &change);
                }
                Err(e) => {
                    println!("Error applying update: {}", e);
                }
            }
        }
    } else {
        println!("Attribute store already up-to-date");
    }

    Ok(())
}
