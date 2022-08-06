use std::env;
use std::io;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use dialoguer::Confirm;
use postgres::{Client, NoTls};
use structopt::StructOpt;

use minerva::attribute_store::{
    load_attribute_store, load_attribute_store_from_file, AddAttributeStore, AttributeStore,
};
use minerva::change::Change;
use minerva::error::{ConfigurationError, Error, RuntimeError};
use minerva::instance::{dump, MinervaInstance};
use minerva::trend_store::{
    analyze_trend_store_part, create_partitions, delete_trend_store, list_trend_stores,
    load_trend_store, load_trend_store_from_file, AddTrendStore,
};

use term_table::{
    row::Row,
    table_cell::{Alignment, TableCell},
    Table, TableStyle,
};

static ENV_MINERVA_INSTANCE_ROOT: &str = "MINERVA_INSTANCE_ROOT";
static ENV_DB_CONN: &str = "MINERVA_DB_CONN";

type CmdResult = Result<(), Error>;

/// Defines the interface for CLI commands
trait Cmd {
    fn run(&self) -> CmdResult;
}

#[derive(Debug, StructOpt)]
struct DeleteOpt {
    id: i32,
}

impl Cmd for DeleteOpt {
    fn run(&self) -> CmdResult {
        println!("Deleting trend store {}", self.id);

        let mut client = connect_db()?;

        let result = delete_trend_store(&mut client, self.id);

        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::Runtime(RuntimeError {
                msg: format!("Error deleting trend store: {}", e),
            })),
        }
    }
}

#[derive(Debug, StructOpt)]
struct TrendStoreCreate {
    #[structopt(help = "trend store definition file")]
    definition: PathBuf,
}

impl Cmd for TrendStoreCreate {
    fn run(&self) -> CmdResult {
        let trend_store = load_trend_store_from_file(&self.definition)?;

        println!("Loaded definition, creating trend store");

        let mut client = connect_db()?;

        let change = AddTrendStore { trend_store };

        change.apply(&mut client)?;

        println!("Created trend store");

        Ok(())
    }
}

#[derive(Debug, StructOpt)]
struct TrendStoreDiff {
    #[structopt(help = "trend store definition file")]
    definition: PathBuf,
}

impl Cmd for TrendStoreDiff {
    fn run(&self) -> CmdResult {
        let trend_store = load_trend_store_from_file(&self.definition)?;

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

                if !changes.is_empty() {
                    println!("Differences with the database");

                    for change in changes {
                        println!("{}", &change);
                    }
                } else {
                    println!("Trend store already up-to-date")
                }

                Ok(())
            }
            Err(e) => Err(Error::Runtime(RuntimeError {
                msg: format!("Error loading trend store: {}", e),
            })),
        }
    }
}

#[derive(Debug, StructOpt)]
struct TrendStoreUpdate {
    #[structopt(help = "trend store definition file")]
    definition: PathBuf,
}

impl Cmd for TrendStoreUpdate {
    fn run(&self) -> CmdResult {
        let trend_store = load_trend_store_from_file(&self.definition)?;

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

                if !changes.is_empty() {
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
            Err(e) => Err(Error::Runtime(RuntimeError {
                msg: format!("Error loading trend store: {}", e),
            })),
        }
    }
}

#[derive(Debug, StructOpt)]
struct TrendStorePartitionCreate {
    #[structopt(
        help="period for which to create partitions",
        long="--ahead-interval",
        parse(try_from_str = humantime::parse_duration)
    )]
    ahead_interval: Option<Duration>,
}

#[derive(Debug, StructOpt)]
enum TrendStorePartition {
    #[structopt(about = "create partitions")]
    Create(TrendStorePartitionCreate),
}

#[derive(Debug, StructOpt)]
struct TrendStoreCheck {
    #[structopt(help = "trend store definition file")]
    definition: PathBuf,
}

#[derive(Debug, StructOpt)]
struct TrendStorePartAnalyze {
    #[structopt(help = "name of trend store part")]
    name: String,
}

impl Cmd for TrendStorePartAnalyze {
    fn run(&self) -> CmdResult {
        let mut client = connect_db()?;

        let result = analyze_trend_store_part(&mut client, &self.name)?;

        println!("Analyzed '{}'", self.name);

        let mut table = Table::new();
        table.style = TableStyle::thin();
        table.separate_rows = false;

        table.add_row(Row::new(vec![
            TableCell::new("Name"),
            TableCell::new("Min"),
            TableCell::new("Max"),
        ]));

        for stat in result.trend_stats {
            table.add_row(Row::new(vec![
                TableCell::new(&stat.name),
                TableCell::new_with_alignment(
                    &stat.min_value.unwrap_or("N/A".into()),
                    1,
                    Alignment::Right,
                ),
                TableCell::new_with_alignment(
                    &stat.max_value.unwrap_or("N/A".into()),
                    1,
                    Alignment::Right,
                ),
            ]));
        }

        println!("{}", table.render());

        Ok(())
    }
}

#[derive(Debug, StructOpt)]
enum TrendStorePartOpt {
    #[structopt(about = "analyze range of values for trends in a trend store part")]
    Analyze(TrendStorePartAnalyze),
}

#[derive(Debug, StructOpt)]
enum TrendStoreOpt {
    #[structopt(about = "list existing trend stores")]
    List,
    #[structopt(about = "create a trend store")]
    Create(TrendStoreCreate),
    #[structopt(about = "show differences for a trend store")]
    Diff(TrendStoreDiff),
    #[structopt(about = "update a trend store")]
    Update(TrendStoreUpdate),
    #[structopt(about = "delete a trend store")]
    Delete(DeleteOpt),
    #[structopt(about = "partition management commands")]
    Partition(TrendStorePartition),
    #[structopt(about = "run sanity checks for trend store")]
    Check(TrendStoreCheck),
    #[structopt(about = "part management commands")]
    Part(TrendStorePartOpt),
}

#[derive(Debug, StructOpt)]
struct AttributeStoreCreate {
    #[structopt(help = "attribute store definition file")]
    definition: PathBuf,
}

#[derive(Debug, StructOpt)]
struct AttributeStoreUpdate {
    #[structopt(help = "attribute store definition file")]
    definition: PathBuf,
}

#[derive(Debug, StructOpt)]
enum AttributeStoreOpt {
    #[structopt(about = "create an attribute store")]
    Create(AttributeStoreCreate),
    #[structopt(about = "update an attribute store")]
    Update(AttributeStoreUpdate),
}

#[derive(Debug, StructOpt)]
struct InitializeOpt {
    #[structopt(long = "--create-partitions", help = "create partitions")]
    create_partitions: bool,
}

impl Cmd for InitializeOpt {
    fn run(&self) -> CmdResult {
        let minerva_instance_root = match env::var(ENV_MINERVA_INSTANCE_ROOT) {
            Ok(v) => PathBuf::from(v),
            Err(e) => {
                return Err(Error::Configuration(ConfigurationError {
                    msg: format!(
                        "Environment variable '{}' could not be read: {}",
                        &ENV_MINERVA_INSTANCE_ROOT, e
                    ),
                }));
            }
        };

        let mut client = connect_db()?;

        println!(
            "Initializing Minerva instance from {}",
            minerva_instance_root.to_string_lossy()
        );

        MinervaInstance::load_from(&minerva_instance_root).initialize(&mut client);

        if self.create_partitions {
            create_partitions(&mut client, None)?;
        }

        Ok(())
    }
}
#[derive(Debug, StructOpt)]
struct DiffOpt {
    #[structopt(
        long = "--with-dir",
        help = "compare with other Minerva instance directory"
    )]
    with_dir: Option<PathBuf>,
}

impl Cmd for DiffOpt {
    fn run(&self) -> CmdResult {
        let minerva_instance_root = match env::var(ENV_MINERVA_INSTANCE_ROOT) {
            Ok(v) => PathBuf::from(v),
            Err(e) => {
                return Err(Error::Configuration(ConfigurationError {
                    msg: format!(
                        "Environment variable '{}' could not be read: {}",
                        &ENV_MINERVA_INSTANCE_ROOT, e
                    ),
                }));
            }
        };

        let instance_def = MinervaInstance::load_from(&minerva_instance_root);

        let other_instance = match &self.with_dir {
            Some(with_dir) => MinervaInstance::load_from(&with_dir),
            None => {
                let mut client = connect_db()?;

                MinervaInstance::load_from_db(&mut client)?
            }
        };

        let changes = other_instance.diff(&instance_def);

        if !changes.is_empty() {
            println!("Differences with database:");

            for change in changes {
                println!("* {}", &change);
            }
        } else {
            println!("Database is up-to-date");
        }

        Ok(())
    }
}

#[derive(Debug, StructOpt)]
struct UpdateOpt {
    #[structopt(short, long)]
    non_interactive: bool,
}

impl Cmd for UpdateOpt {
    fn run(&self) -> CmdResult {
        let mut client = connect_db()?;

        print!("Reading Minerva instance from database... ");
        io::stdout().flush().unwrap();
        let instance_db = MinervaInstance::load_from_db(&mut client)?;
        print!("Ok\n");

        let minerva_instance_root = match env::var(ENV_MINERVA_INSTANCE_ROOT) {
            Ok(v) => PathBuf::from(v),
            Err(e) => {
                return Err(Error::Configuration(ConfigurationError {
                    msg: format!(
                        "Environment variable '{}' could not be read: {}",
                        &ENV_MINERVA_INSTANCE_ROOT, e
                    ),
                }));
            }
        };

        print!(
            "Reading Minerva instance from '{}'... ",
            &minerva_instance_root.to_string_lossy()
        );
        io::stdout().flush().unwrap();
        let instance_def = MinervaInstance::load_from(&minerva_instance_root);
        print!("Ok\n");

        update(
            &mut client,
            &instance_db,
            &instance_def,
            !self.non_interactive,
        )
    }
}

#[derive(Debug, StructOpt)]
enum Opt {
    #[structopt(about = "Complete dump of a Minerva instance")]
    Dump,
    #[structopt(about = "Create a diff between Minerva instance definitions")]
    Diff(DiffOpt),
    #[structopt(about = "Update a Minerva database from an instance definition")]
    Update(UpdateOpt),
    #[structopt(about = "Initialize a complete Minerva instance")]
    Initialize(InitializeOpt),
    #[structopt(about = "Manage trend stores")]
    TrendStore(TrendStoreOpt),
    #[structopt(about = "Manage attribute stores")]
    AttributeStore(AttributeStoreOpt),
}

fn main() {
    let opt = Opt::from_args();

    let result = match opt {
        Opt::Dump => run_dump_cmd(),
        Opt::Diff(diff) => diff.run(),
        Opt::Update(update) => update.run(),
        Opt::Initialize(initialize) => initialize.run(),
        Opt::TrendStore(trend_store) => match trend_store {
            TrendStoreOpt::List => run_trend_store_list_cmd(),
            TrendStoreOpt::Create(create) => create.run(),
            TrendStoreOpt::Diff(diff) => diff.run(),
            TrendStoreOpt::Update(update) => update.run(),
            TrendStoreOpt::Delete(delete) => run_trend_store_delete_cmd(&delete),
            TrendStoreOpt::Partition(partition) => match partition {
                TrendStorePartition::Create(create) => {
                    run_trend_store_partition_create_cmd(&create)
                }
            },
            TrendStoreOpt::Check(check) => run_trend_store_check_cmd(&check),
            TrendStoreOpt::Part(part) => match part {
                TrendStorePartOpt::Analyze(analyze) => analyze.run(),
            },
        },
        Opt::AttributeStore(attribute_store) => match attribute_store {
            AttributeStoreOpt::Create(args) => run_attribute_store_create_cmd(&args),
            AttributeStoreOpt::Update(args) => run_attribute_store_update_cmd(&args),
        },
    };

    if let Err(e) = result {
        println!("{}", e);
    }
}

fn connect_db() -> Result<Client, Error> {
    let conn_params = match env::var(ENV_DB_CONN) {
        Ok(value) => String::from(value),
        Err(_) => {
            // No single environment variable set, let's check for psql settings
            let pg_host = env::var("PGHOST").unwrap_or("localhost".into());
            let pg_port = env::var("PGPORT").unwrap_or("5432".into());
            let pg_user = env::var("PGUSER").unwrap_or("postgres".into());
            let pg_password = env::var("PGPASSWORD");
            let pg_database = env::var("PGDATABASE").unwrap_or("postgres".into());

            match pg_password {
                Ok(password) => format!(
                    "postgresql://{}:{}@{}:{}/{}",
                    pg_user, password, pg_host, pg_port, pg_database
                ),
                Err(_) => format!(
                    "postgresql://{}@{}:{}/{}",
                    pg_user, pg_host, pg_port, pg_database
                ),
            }
        }
    };

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
        Err(e) => Err(Error::Runtime(RuntimeError {
            msg: format!("Error deleting trend store: {}", e),
        })),
    }
}

fn run_trend_store_check_cmd(args: &TrendStoreCheck) -> CmdResult {
    let trend_store = load_trend_store_from_file(&args.definition)?;

    for trend_store_part in &trend_store.parts {
        let count = trend_store
            .parts
            .iter()
            .filter(|&p| p.name == trend_store_part.name)
            .count();

        if count > 1 {
            println!(
                "Error: {} trend store parts with name '{}'",
                count, &trend_store_part.name
            );
        }
    }

    Ok(())
}

fn run_attribute_store_create_cmd(args: &AttributeStoreCreate) -> CmdResult {
    let attribute_store: AttributeStore = load_attribute_store_from_file(&args.definition)?;

    println!("Loaded definition, creating attribute store");

    let mut client = connect_db()?;

    let change = AddAttributeStore { attribute_store };

    let result = change.apply(&mut client);

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

fn run_attribute_store_update_cmd(args: &AttributeStoreUpdate) -> CmdResult {
    let attribute_store: AttributeStore = load_attribute_store_from_file(&args.definition)?;

    println!("Loaded definition, updating attribute store");

    let mut client = connect_db()?;

    let attribute_store_db = load_attribute_store(
        &mut client,
        &attribute_store.data_source,
        &attribute_store.entity_type,
    )?;

    let changes = attribute_store_db.diff(&attribute_store);

    if !changes.is_empty() {
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

fn run_dump_cmd() -> CmdResult {
    let mut client = connect_db()?;

    dump(&mut client);

    Ok(())
}

fn run_trend_store_partition_create_cmd(args: &TrendStorePartitionCreate) -> CmdResult {
    let mut client = connect_db()?;

    create_partitions(&mut client, args.ahead_interval)?;

    println!("Created partitions");
    Ok(())
}

fn update(
    client: &mut Client,
    db_instance: &MinervaInstance,
    other: &MinervaInstance,
    interactive: bool,
) -> CmdResult {
    let changes = db_instance.diff(other);

    println!("Applying changes:");

    for change in changes {
        println!("* {}", change);

        if (!interactive)
            || Confirm::new()
                .with_prompt("Apply change?")
                .interact()
                .map_err(|e| {
                    Error::Runtime(RuntimeError {
                        msg: format!("Could not process input: {}", e),
                    })
                })?
        {
            match change.apply(client) {
                Ok(message) => println!("> {}", &message),
                Err(err) => println!("! Error applying change: {}", &err),
            }
        }
    }

    Ok(())
}
