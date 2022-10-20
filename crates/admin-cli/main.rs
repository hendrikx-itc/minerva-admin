use std::env;
use std::io;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use chrono::DateTime;
use chrono::FixedOffset;
use dialoguer::Confirm;

use async_trait::async_trait;

use rustls::ClientConfig as RustlsClientConfig;
use structopt::StructOpt;
use tokio;
use tokio_postgres::{Config, config::SslMode};
use tokio_postgres::{Client, NoTls};
use tokio_postgres_rustls::MakeRustlsConnect;

use minerva::attribute_store::{
    load_attribute_store, load_attribute_store_from_file, AddAttributeStore, AttributeStore,
};
use minerva::change::Change;
use minerva::error::{ConfigurationError, Error, RuntimeError};
use minerva::instance::{dump, MinervaInstance};
use minerva::trend_materialization;
use minerva::trend_materialization::{
    trend_materialization_from_config, AddTrendMaterialization, UpdateTrendMaterialization,
};
use minerva::trend_store::{
    analyze_trend_store_part, create_partitions, create_partitions_for_timestamp,
    delete_trend_store, list_trend_stores, load_trend_store, load_trend_store_from_file,
    AddTrendStore,
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
#[async_trait]
trait Cmd {
    async fn run(&self) -> CmdResult;
}

#[derive(Debug, StructOpt)]
struct DeleteOpt {
    id: i32,
}

#[derive(Debug, StructOpt)]
struct TrendStoreCreate {
    #[structopt(help = "trend store definition file")]
    definition: PathBuf,
}

#[async_trait]
impl Cmd for TrendStoreCreate {
    async fn run(&self) -> CmdResult {
        let trend_store = load_trend_store_from_file(&self.definition)?;

        println!("Loaded definition, creating trend store");

        let mut client = connect_db().await?;

        let change = AddTrendStore { trend_store };

        change.apply(&mut client).await?;

        println!("Created trend store");

        Ok(())
    }
}

#[derive(Debug, StructOpt)]
struct TrendStoreDiff {
    #[structopt(help = "trend store definition file")]
    definition: PathBuf,
}

#[async_trait]
impl Cmd for TrendStoreDiff {
    async fn run(&self) -> CmdResult {
        let trend_store = load_trend_store_from_file(&self.definition)?;

        let mut client = connect_db().await?;

        let result = load_trend_store(
            &mut client,
            &trend_store.data_source,
            &trend_store.entity_type,
            &trend_store.granularity,
        )
        .await;

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

#[async_trait]
impl Cmd for TrendStoreUpdate {
    async fn run(&self) -> CmdResult {
        let trend_store = load_trend_store_from_file(&self.definition)?;

        let mut client = connect_db().await?;

        let result = load_trend_store(
            &mut client,
            &trend_store.data_source,
            &trend_store.entity_type,
            &trend_store.granularity,
        )
        .await;

        match result {
            Ok(trend_store_db) => {
                let changes = trend_store_db.diff(&trend_store);

                if !changes.is_empty() {
                    println!("Updating trend store");

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
    #[structopt(
        help="timestamp for which to create partitions",
        long="--for-timestamp",
        parse(try_from_str = DateTime::parse_from_rfc3339)
    )]
    for_timestamp: Option<DateTime<FixedOffset>>,
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

#[async_trait]
impl Cmd for TrendStorePartAnalyze {
    async fn run(&self) -> CmdResult {
        let mut client = connect_db().await?;

        let result = analyze_trend_store_part(&mut client, &self.name).await?;

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

#[async_trait]
impl Cmd for InitializeOpt {
    async fn run(&self) -> CmdResult {
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

        let mut client = connect_db().await?;

        println!(
            "Initializing Minerva instance from {}",
            minerva_instance_root.to_string_lossy()
        );

        MinervaInstance::load_from(&minerva_instance_root)
            .initialize(&mut client)
            .await;

        if self.create_partitions {
            create_partitions(&mut client, None).await?;
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

#[async_trait]
impl Cmd for DiffOpt {
    async fn run(&self) -> CmdResult {
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

        let from_instance_descr = format!("dir('{}')", minerva_instance_root.to_string_lossy());
        let to_instance_descr: String;

        let instance_def = MinervaInstance::load_from(&minerva_instance_root);

        let other_instance = match &self.with_dir {
            Some(with_dir) => {
                to_instance_descr = format!("dir('{}')", with_dir.to_string_lossy());
                MinervaInstance::load_from(&with_dir)
            },
            None => {
                let db_config = get_db_config()?;

                to_instance_descr = format!("database('{:?}')", db_config);

                let mut client = connect_to_db(&db_config).await?;

                MinervaInstance::load_from_db(&mut client).await?
            }
        };

        let changes = other_instance.diff(&instance_def);

        if !changes.is_empty() {
            println!("Differences {} -> {}", from_instance_descr, to_instance_descr);

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

#[async_trait]
impl Cmd for UpdateOpt {
    async fn run(&self) -> CmdResult {
        let mut client = connect_db().await?;

        print!("Reading Minerva instance from database... ");
        io::stdout().flush().unwrap();
        let instance_db = MinervaInstance::load_from_db(&mut client).await?;
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
        .await
    }
}

#[derive(Debug, StructOpt)]
struct TrendMaterializationCreate {
    #[structopt(help = "trend materialization definition file")]
    definition: PathBuf,
}

#[async_trait]
impl Cmd for TrendMaterializationCreate {
    async fn run(&self) -> CmdResult {
        let trend_materialization =
            trend_materialization::trend_materialization_from_config(&self.definition)?;

        println!("Loaded definition, creating trend materialization");
        let mut client = connect_db().await?;

        let change = AddTrendMaterialization {
            trend_materialization,
        };

        let result = change.apply(&mut client).await;

        match result {
            Ok(_) => {
                println!("Created trend materialization");

                Ok(())
            }
            Err(e) => Err(Error::Runtime(RuntimeError {
                msg: format!("Error creating trend materialization: {}", e),
            })),
        }
    }
}

#[derive(Debug, StructOpt)]
struct TrendMaterializationUpdate {
    #[structopt(help = "trend materialization definition file")]
    definition: PathBuf,
}

#[async_trait]
impl Cmd for TrendMaterializationUpdate {
    async fn run(&self) -> CmdResult {
        let trend_materialization = trend_materialization_from_config(&self.definition)?;

        println!("Loaded definition, updating trend materialization");
        let mut client = connect_db().await?;

        let change = UpdateTrendMaterialization {
            trend_materialization,
        };

        let result = change.apply(&mut client).await;

        match result {
            Ok(_) => {
                println!("Updated trend materialization");

                Ok(())
            }
            Err(e) => Err(Error::Runtime(RuntimeError {
                msg: format!("Error updating trend materialization: {}", e),
            })),
        }
    }
}

#[derive(Debug, StructOpt)]
enum TrendMaterializationOpt {
    #[structopt(about = "create a trend materialization")]
    Create(TrendMaterializationCreate),
    #[structopt(about = "update a trend materialization")]
    Update(TrendMaterializationUpdate),
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
    #[structopt(about = "Manage trend materializations")]
    TrendMaterialization(TrendMaterializationOpt),
}

#[tokio::main]
async fn main() {
    let opt = Opt::from_args();

    let result = match opt {
        Opt::Dump => run_dump_cmd().await,
        Opt::Diff(diff) => diff.run().await,
        Opt::Update(update) => update.run().await,
        Opt::Initialize(initialize) => initialize.run().await,
        Opt::TrendStore(trend_store) => match trend_store {
            TrendStoreOpt::List => run_trend_store_list_cmd().await,
            TrendStoreOpt::Create(create) => create.run().await,
            TrendStoreOpt::Diff(diff) => diff.run().await,
            TrendStoreOpt::Update(update) => update.run().await,
            TrendStoreOpt::Delete(delete) => run_trend_store_delete_cmd(&delete).await,
            TrendStoreOpt::Partition(partition) => match partition {
                TrendStorePartition::Create(create) => {
                    run_trend_store_partition_create_cmd(&create).await
                }
            },
            TrendStoreOpt::Check(check) => run_trend_store_check_cmd(&check),
            TrendStoreOpt::Part(part) => match part {
                TrendStorePartOpt::Analyze(analyze) => analyze.run().await,
            },
        },
        Opt::AttributeStore(attribute_store) => match attribute_store {
            AttributeStoreOpt::Create(args) => run_attribute_store_create_cmd(&args).await,
            AttributeStoreOpt::Update(args) => run_attribute_store_update_cmd(&args).await,
        },
        Opt::TrendMaterialization(trend_materialization) => match trend_materialization {
            TrendMaterializationOpt::Create(trend_materialization_create) => {
                trend_materialization_create.run().await
            }
            TrendMaterializationOpt::Update(trend_materialization_update) => {
                trend_materialization_update.run().await
            }
        },
    };

    if let Err(e) = result {
        println!("{}", e);
    }
}

fn get_db_config() -> Result<Config, Error> {
    let config = match env::var(ENV_DB_CONN) {
        Ok(value) => Config::new().options(&value).clone(),
        Err(_) => {
            // No single environment variable set, let's check for psql settings
            let port: u16 = env::var("PGPORT").unwrap_or("5432".into()).parse().unwrap();
            let mut config = Config::new();

            let env_sslmode = env::var("PGSSLMODE").unwrap_or("prefer".into());

            let sslmode = match env_sslmode.to_lowercase().as_str() {
                "disable" => SslMode::Disable,
                "prefer" => SslMode::Prefer,
                "require" => SslMode::Require,
                _ => return Err(Error::Configuration(ConfigurationError { msg: format!("Unsupported SSL mode '{}'", &env_sslmode) }))
            };

            let config = config
                .host(&env::var("PGHOST").unwrap_or("localhost".into()))
                .port(port)
                .user(&env::var("PGUSER").unwrap_or("postgres".into()))
                .dbname(&env::var("PGDATABASE").unwrap_or("postgres".into()))
                .ssl_mode(sslmode);

            let pg_password = env::var("PGPASSWORD");

            match pg_password {
                Ok(password) => config.password(password).clone(),
                Err(_) => config.clone(),
            }
        }
    };

    Ok(config)
}

async fn connect_db() -> Result<Client, Error> {
    connect_to_db(&get_db_config()?).await
}

async fn connect_to_db(config: &Config) -> Result<Client, Error> {
    let client = if true {
        let mut roots = rustls::RootCertStore::empty();

        for cert in rustls_native_certs::load_native_certs().expect("could not load platform certs")
        {
            roots.add(&rustls::Certificate(cert.0)).unwrap();
        }

        let tls_config = RustlsClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let tls = MakeRustlsConnect::new(tls_config);

        let (client, connection) = config.connect(tls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        client
    } else {
        let (client, connection) = config.connect(NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        client
    };

    Ok(client)
}

async fn run_trend_store_list_cmd() -> CmdResult {
    let mut client = connect_db().await?;

    let trend_stores = list_trend_stores(&mut client).await.unwrap();

    for trend_store in trend_stores {
        println!("{}", &trend_store);
    }

    Ok(())
}

async fn run_trend_store_delete_cmd(args: &DeleteOpt) -> CmdResult {
    println!("Deleting trend store {}", args.id);

    let mut client = connect_db().await?;

    let result = delete_trend_store(&mut client, args.id).await;

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

async fn run_dump_cmd() -> CmdResult {
    let mut client = connect_db().await?;

    dump(&mut client).await;

    Ok(())
}

async fn run_trend_store_partition_create_cmd(args: &TrendStorePartitionCreate) -> CmdResult {
    let mut client = connect_db().await?;

    if let Some(for_timestamp) = args.for_timestamp {
        create_partitions_for_timestamp(&mut client, for_timestamp).await?;
    } else {
        create_partitions(&mut client, args.ahead_interval).await?;
    }

    println!("Created partitions");
    Ok(())
}

async fn update(
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
            match change.apply(client).await {
                Ok(message) => println!("> {}", &message),
                Err(err) => println!("! Error applying change: {}", &err),
            }
        }
    }

    Ok(())
}
