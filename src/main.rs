use std::env;
use std::path::PathBuf;

use structopt::StructOpt;
use postgres::{Client, NoTls};

mod minerva;
mod error;
use error::{Error, ConfigurationError, RuntimeError};

use minerva::attribute_store::{load_attribute_store, AddAttributeStore, AttributeStore};
use minerva::change::Change;
use minerva::instance::{dump, MinervaInstance};
use minerva::trend_store::{
    delete_trend_store, list_trend_stores, load_trend_store, AddTrendStore, load_trend_store_from_file
};

static ENV_MINERVA_INSTANCE_ROOT: &str = "MINERVA_INSTANCE_ROOT";
static ENV_DB_CONN: &str = "MINERVA_DB_CONN";

type CmdResult = Result<(), Error>;

#[derive(Debug, StructOpt)]
struct DeleteOpt {
    id: i32
}

#[derive(Debug, StructOpt)]
struct TrendStoreCreate {
    #[structopt(help="trend store definition file")]
    definition: PathBuf
}

#[derive(Debug, StructOpt)]
struct TrendStoreDiff {
    #[structopt(help="trend store definition file")]
    definition: PathBuf
}

#[derive(Debug, StructOpt)]
struct TrendStoreUpdate {
    #[structopt(help="trend store definition file")]
    definition: PathBuf
}

#[derive(Debug, StructOpt)]
enum TrendStoreOpt {
    #[structopt(about="list existing trend stores")]
    List,
    #[structopt(about="create a trend store")]
    Create(TrendStoreCreate),
    #[structopt(about="show differences for a trend store")]
    Diff(TrendStoreDiff),
    #[structopt(about="update a trend store")]
    Update(TrendStoreUpdate),
    #[structopt(about="delete a trend store")]
    Delete(DeleteOpt),
}

#[derive(Debug, StructOpt)]
struct AttributeStoreCreate {
    #[structopt(help="attribute store definition file")]
    definition: PathBuf
}
#[derive(Debug, StructOpt)]
struct AttributeStoreUpdate {
    #[structopt(help="attribute store definition file")]
    definition: PathBuf
}

#[derive(Debug, StructOpt)]
enum AttributeStoreOpt {
    #[structopt(about="create an attribute store")]
    Create(AttributeStoreCreate),
    #[structopt(about="update an attribute store")]
    Update(AttributeStoreUpdate),
}

#[derive(Debug, StructOpt)]
enum Opt {
    #[structopt(about="command for complete dump of a Minerva instance")]
    Dump,
    #[structopt(about="command for creating a diff between Minerva instance definition and database")]
    Diff,
    #[structopt(about="command for updating a Minerva database from an instance definition")]
    Update,
    #[structopt(about="command for complete initialization of a Minerva instance")]
    Initialize,
    #[structopt(about="manage trend stores")]
    TrendStore(TrendStoreOpt),
    #[structopt(about="manage attribute stores")]
    AttributeStore(AttributeStoreOpt),
}

fn main() {
    let opt = Opt::from_args();

    let result = match opt {
        Opt::Dump => run_dump_cmd(),
        Opt::Diff => run_diff_cmd(),
        Opt::Update => run_update_cmd(),
        Opt::Initialize => run_initialize_cmd(),
        Opt::TrendStore(trend_store) => {
            match trend_store {
                TrendStoreOpt::List => run_trend_store_list_cmd(),
                TrendStoreOpt::Create(create) => run_trend_store_create_cmd(&create),
                TrendStoreOpt::Diff(diff) => run_trend_store_diff_cmd(&diff),
                TrendStoreOpt::Update(update) => run_trend_store_update_cmd(&update),
                TrendStoreOpt::Delete(delete) => run_trend_store_delete_cmd(&delete),
            }
        },
        Opt::AttributeStore(attribute_store) => {
            match attribute_store {
                AttributeStoreOpt::Create(args) => run_attribute_store_create_cmd(&args),
                AttributeStoreOpt::Update(args) => run_attribute_store_update_cmd(&args),
            }
        }
    };

    if let Err(e) = result {
        println!("{}", e);
    }
}

fn connect_db() -> Result<Client, Error> {
    let conn_params = env::var(ENV_DB_CONN).map_err(|e| {
        ConfigurationError::from_msg(format!("{}", e))
    })?;

    let client = Client::connect(&conn_params, NoTls)?;

    Ok(client)
}

fn run_trend_store_list_cmd() -> CmdResult {
    let mut client = connect_db()?;

    let trend_stores = list_trend_stores(&mut client).unwrap();

    for trend_store in trend_stores {
        println!("{}", &trend_store);
    }

    Ok(())
}

fn run_trend_store_delete_cmd(args: &DeleteOpt) -> CmdResult {
    println!("Deleting trend store {}", args.id);

    let mut client = connect_db()?;

    let result = delete_trend_store(&mut client, args.id);

    match result {
        Ok(_) => Ok(()),
        Err(e) => {
            Err(Error::Runtime(RuntimeError{ msg: format!("Error deleting trend store: {}", e) } ))
        }
    }
}

fn run_trend_store_create_cmd(args: &TrendStoreCreate) -> CmdResult {
    let trend_store = load_trend_store_from_file(&args.definition)?;

    println!("Loaded definition, creating trend store");

    let mut client = connect_db()?;

    let change = AddTrendStore {
        trend_store: trend_store,
    };

    change.apply(&mut client)?;

    println!("Created trend store");

    Ok(())
}

fn run_trend_store_diff_cmd(args: &TrendStoreDiff) -> CmdResult {
    let trend_store = load_trend_store_from_file(&args.definition)?;

    let mut client = connect_db()?;

    let result = load_trend_store(
        &mut client,
        &trend_store.data_source,
        &trend_store.entity_type,
        &trend_store.granularity,
    );

    match result {
        Ok(trend_store_db) => {
            let changes = trend_store_db.diff(&trend_store);

            if changes.len() > 0 {
                println!("Differences with the database");

                for change in changes {
                    println!("{}", &change);
                }
            } else {
                println!("Trend store already up-to-date")
            }

            Ok(())
        }
        Err(e) => {
            Err(Error::Runtime(RuntimeError { msg: format!("Error loading trend store: {}", e)}))
        }
    }
}

fn run_trend_store_update_cmd(args: &TrendStoreUpdate) -> CmdResult {
    let trend_store = load_trend_store_from_file(&args.definition)?;

    let mut client = connect_db()?;

    let result = load_trend_store(
        &mut client,
        &trend_store.data_source,
        &trend_store.entity_type,
        &trend_store.granularity,
    );

    match result {
        Ok(trend_store_db) => {
            let changes = trend_store_db.diff(&trend_store);

            if changes.len() > 0 {
                println!("Updating trend store");

                for change in changes {
                    let apply_result = change.apply(&mut client);

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
                println!("Trend store already up-to-date")
            }

            Ok(())
        }
        Err(e) => {
            Err(Error::Runtime(RuntimeError { msg: format!("Error loading trend store: {}", e)}))
        }
    }
}

fn run_attribute_store_create_cmd(args: &AttributeStoreCreate) -> CmdResult {
    let f = std::fs::File::open(&args.definition).unwrap();
    let attribute_store: AttributeStore = serde_yaml::from_reader(f).unwrap();

    println!("Loaded definition, creating attribute store");

    let mut client = connect_db()?;

    let change = AddAttributeStore {
        attribute_store: attribute_store,
    };

    let result = change.apply(&mut client);

    match result {
        Ok(_) => {
            println!("Created attribute store");

            Ok(())
        }
        Err(e) => {
            Err(Error::Runtime(RuntimeError { msg: format!("Error creating attribute store: {}", e) }))
        }
    }
}

fn run_attribute_store_update_cmd(args: &AttributeStoreUpdate) -> CmdResult {
    let f = std::fs::File::open(&args.definition).unwrap();
    let attribute_store: AttributeStore = serde_yaml::from_reader(f).unwrap();

    println!("Loaded definition, updating attribute store");

    let mut client = connect_db()?;

    let attribute_store_db = load_attribute_store(
        &mut client,
        &attribute_store.data_source,
        &attribute_store.entity_type,
    )?;

    let changes = attribute_store_db.diff(&attribute_store);

    if changes.len() > 0 {
        println!("Updating attribute store");

        for change in changes {
            let apply_result = change.apply(&mut client);

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

fn run_initialize_cmd() -> CmdResult {
    let mut client = connect_db()?;
    let minerva_instance_root = env::var(ENV_MINERVA_INSTANCE_ROOT).unwrap();

    MinervaInstance::initialize_from(&mut client, &minerva_instance_root);

    Ok(())
}

fn run_dump_cmd() -> CmdResult {
    let mut client = connect_db()?;

    dump(&mut client);

    Ok(())
}

fn run_diff_cmd() -> CmdResult {
    let mut client = connect_db()?;

    let instance_db = MinervaInstance::load_from_db(&mut client)?;

    let minerva_instance_root = env::var(ENV_MINERVA_INSTANCE_ROOT).unwrap();

    let instance_def = MinervaInstance::load_from(&minerva_instance_root);

    let changes = instance_db.diff(&instance_def);

    if changes.len() > 0 {
        println!("Differences with database:");

        for change in changes {
            println!("* {}", &change);
        }
    } else {
        println!("Database is up-to-date");
    }

    Ok(())
}

fn run_update_cmd() -> CmdResult {
    let mut client = connect_db()?;

    let instance_db = MinervaInstance::load_from_db(&mut client)?;

    let minerva_instance_root = match env::var(ENV_MINERVA_INSTANCE_ROOT) {
        Ok(v) => v,
        Err(e) => {
            return Err(Error::Configuration(ConfigurationError {msg: format!("Environment variable '{}' could not be read: {}", &ENV_MINERVA_INSTANCE_ROOT, e)}) );
        }
    };

    let instance_def = MinervaInstance::load_from(&minerva_instance_root);

    instance_db.update(&mut client, &instance_def)
}
