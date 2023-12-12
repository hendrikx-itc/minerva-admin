use clap::{Parser, Subcommand};

pub mod commands;

use crate::commands::attributestore::AttributeStoreOptCommands;
use crate::commands::common::Cmd;
use crate::commands::diff::DiffOpt;
use crate::commands::schema::SchemaOpt;
use crate::commands::dump::DumpOpt;
use crate::commands::initialize::InitializeOpt;
use crate::commands::loaddata::LoadDataOpt;
use crate::commands::trendmaterialization::TrendMaterializationOpt;
use crate::commands::trendstore::TrendStoreOpt;
use crate::commands::trigger::TriggerOpt;
use crate::commands::update::UpdateOpt;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[command(about = "Show the definition used for initializing a new Minerva database")]
    Schema(SchemaOpt),
    #[command(about = "Complete dump of a Minerva instance")]
    Dump(DumpOpt),
    #[command(about = "Create a diff between Minerva instance definitions")]
    Diff(DiffOpt),
    #[command(about = "Update a Minerva database from an instance definition")]
    Update(UpdateOpt),
    #[command(about = "Initialize a complete Minerva instance")]
    Initialize(InitializeOpt),
    #[command(about = "Manage trend stores")]
    TrendStore(TrendStoreOpt),
    #[command(about = "Manage triggers")]
    Trigger(TriggerOpt),
    #[command(about = "Manage attribute stores")]
    AttributeStore(AttributeStoreOptCommands),
    #[command(about = "Manage trend materrializations")]
    TrendMaterialization(TrendMaterializationOpt),
    #[command(about = "Load data into Minerva database")]
    LoadData(LoadDataOpt),
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Schema(schema) => schema.run().await,
        Commands::Dump(dump) => dump.run().await,
        Commands::Diff(diff) => diff.run().await,
        Commands::Update(update) => update.run().await,
        Commands::Initialize(initialize) => initialize.run().await,
        Commands::TrendStore(trend_store) => trend_store.run().await,
        Commands::Trigger(trigger) => trigger.run().await,
        Commands::AttributeStore(attribute_store) => attribute_store.run().await,
        Commands::TrendMaterialization(trend_materialization) => trend_materialization.run().await,
        Commands::LoadData(load_data) => load_data.run().await,
    };

    if let Err(e) = result {
        println!("{e}");
    }
}
