use structopt::StructOpt;

pub mod commands;

use crate::commands::attributestore::AttributeStoreOpt;
use crate::commands::common::Cmd;
use crate::commands::diff::DiffOpt;
use crate::commands::dump::DumpOpt;
use crate::commands::initialize::InitializeOpt;
use crate::commands::loaddata::LoadDataOpt;
use crate::commands::trendmaterialization::TrendMaterializationOpt;
use crate::commands::trendstore::TrendStoreOpt;
use crate::commands::trigger::TriggerOpt;
use crate::commands::update::UpdateOpt;

#[derive(Debug, StructOpt)]
enum Opt {
    #[structopt(about = "Complete dump of a Minerva instance")]
    Dump(DumpOpt),
    #[structopt(about = "Create a diff between Minerva instance definitions")]
    Diff(DiffOpt),
    #[structopt(about = "Update a Minerva database from an instance definition")]
    Update(UpdateOpt),
    #[structopt(about = "Initialize a complete Minerva instance")]
    Initialize(InitializeOpt),
    #[structopt(about = "Manage trend stores")]
    TrendStore(TrendStoreOpt),
    #[structopt(about = "Manage triggers")]
    Trigger(TriggerOpt),
    #[structopt(about = "Manage attribute stores")]
    AttributeStore(AttributeStoreOpt),
    #[structopt(about = "Manage trend materializations")]
    TrendMaterialization(TrendMaterializationOpt),
    #[structopt(about = "Load data into Minerva database")]
    LoadData(LoadDataOpt),
}

#[tokio::main]
async fn main() {
    let opt = Opt::from_args();

    let result = match opt {
        Opt::Dump(dump) => dump.run().await,
        Opt::Diff(diff) => diff.run().await,
        Opt::Update(update) => update.run().await,
        Opt::Initialize(initialize) => initialize.run().await,
        Opt::TrendStore(trend_store) => trend_store.run().await,
        Opt::Trigger(trigger) => trigger.run().await,
        Opt::AttributeStore(attribute_store) => attribute_store.run().await,
        Opt::TrendMaterialization(trend_materialization) => trend_materialization.run().await,
        Opt::LoadData(load_data) => load_data.run().await,
    };

    if let Err(e) = result {
        println!("{e}");
    }
}
