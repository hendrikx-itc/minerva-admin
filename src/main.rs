use std::env;

use clap::{crate_authors, crate_name, crate_version, App, Arg, SubCommand, ArgMatches};
use postgres::{Client, NoTls};

mod minerva;

use minerva::trend_store::{TrendStore, AddTrendStore, delete_trend_store, list_trend_stores, load_trend_store};
use minerva::instance::{MinervaInstance, dump};
use minerva::change::Change;

static ENV_MINERVA_INSTANCE_ROOT: &str = "MINERVA_INSTANCE_ROOT";
static ENV_DB_CONN: &str = "MINERVA_DB_CONN";

fn main() {
    let conn_params = env::var(ENV_DB_CONN).unwrap();
    //let conn_params = "host=localhost port=16050 user=postgres password=password dbname=minerva";

    let trend_store_list_cmd = SubCommand::with_name("list")
        .about("list existing trend stores");

    let trend_store_create_cmd = SubCommand::with_name("create")
        .about("create a trend store")
        .arg(
            Arg::with_name("definition")
            .required(true)
            .help("trend store definition file")
        );

    let trend_store_delete_cmd = SubCommand::with_name("delete")
        .about("delete a trend store")
        .arg(Arg::with_name("id"));

    let trend_store_diff_cmd = SubCommand::with_name("diff")
        .about("show differences for a trend store")
        .arg(
            Arg::with_name("definition")
            .required(true)
            .help("trend store definition file")
        );

    let trend_store_update_cmd = SubCommand::with_name("update")
        .about("update a trend store")
        .arg(
            Arg::with_name("definition")
            .required(true)
            .help("trend store definition file")
        );

    let trend_store_cmd = SubCommand::with_name("trend-store")
        .about("manage trend stores")
        .arg(
            Arg::with_name("debug")
                .short("d")
                .help("print debug information verbosely"),
        )
        .subcommand(trend_store_list_cmd)
        .subcommand(trend_store_create_cmd)
        .subcommand(trend_store_delete_cmd)
        .subcommand(trend_store_diff_cmd)
        .subcommand(trend_store_update_cmd);

    let initialize_cmd = SubCommand::with_name("initialize")
        .about("command for complete initialization of a Minerva instance");

    let dump_cmd = SubCommand::with_name("dump")
        .about("command for complete dump of a Minerva instance");

    let diff_cmd = SubCommand::with_name("diff")
        .about("command for creating a diff between Minerva instance definition and database");

    let update_cmd = SubCommand::with_name("update")
        .about("command for updating a Minerva database from an instance definition");

    let matches = App::new(crate_name!())
        .version(crate_version!())
        .author(crate_authors!("\n"))
        .about("Administer Minerva instances")
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file")
                .takes_value(true),
        )
        .subcommand(trend_store_cmd)
        .subcommand(initialize_cmd)
        .subcommand(dump_cmd)
        .subcommand(diff_cmd)
        .subcommand(update_cmd)
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("initialize") {
        run_initialize_cmd(matches, &conn_params);
    }
    if let Some(matches) = matches.subcommand_matches("dump") {
        run_dump_cmd(matches, &conn_params);
    }
    if let Some(matches) = matches.subcommand_matches("diff") {
        run_diff_cmd(matches, &conn_params);
    }
    if let Some(matches) = matches.subcommand_matches("update") {
        run_update_cmd(matches, &conn_params);
    }
    if let Some(matches) = matches.subcommand_matches("trend-store") {
        if let Some(_matches) = matches.subcommand_matches("list") {
            let mut client = Client::connect(&conn_params, NoTls).unwrap();

            let trend_stores = list_trend_stores(&mut client).unwrap();

            for trend_store in trend_stores {
                println!("{}", &trend_store);
            }
        }

        if let Some(matches) = matches.subcommand_matches("create") {
            let definition = matches.value_of("definition").unwrap();

            let f = std::fs::File::open(&definition).unwrap();
            let trend_store: TrendStore = serde_yaml::from_reader(f).unwrap();

            println!("Loaded definition, creating trend store");

            let mut client = Client::connect(&conn_params, NoTls).unwrap();

            let change = AddTrendStore {
                trend_store: trend_store
            };

            let result = change.apply(&mut client);

            match result {
                Ok(_) => {
                    println!("Created trend store");
                },
                Err(e) => {
                    println!("Error creating trend store: {}", e);
                }
            }
        }

        if let Some(matches) = matches.subcommand_matches("delete") {
            let id_str = matches.value_of("id").unwrap();
            let id: i32 = id_str.parse::<i32>().unwrap();

            println!("Deleting trend store {}", id);

            let mut client = Client::connect(&conn_params, NoTls).unwrap();

            let result = delete_trend_store(&mut client, id);

            match result {
                Ok(_) => {},
                Err(e) => {
                    println!("Error deleting trend store: {}", e);
                }
            }
        }

        if let Some(matches) = matches.subcommand_matches("diff") {
            run_trend_store_diff_cmd(matches, &conn_params);
        }

        if let Some(matches) = matches.subcommand_matches("update") {
            run_trend_store_update_cmd(matches, &conn_params);
        }
    }
}

fn run_trend_store_diff_cmd(matches: &ArgMatches, conn_params: &str) {
    let definition = matches.value_of("definition").unwrap();

    let f = std::fs::File::open(&definition).unwrap();
    let trend_store: TrendStore = serde_yaml::from_reader(f).unwrap();

    let mut client = Client::connect(&conn_params, NoTls).unwrap();

    let result = load_trend_store(
        &mut client, &trend_store.data_source, &trend_store.entity_type, &trend_store.granularity
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
        },
        Err(e) => {
            println!("Error loading trend store: {}", e);
        }
    }
}

fn run_trend_store_update_cmd(matches: &ArgMatches, conn_params: &str) {
    let definition = matches.value_of("definition").unwrap();

    let f = std::fs::File::open(&definition).unwrap();
    let trend_store: TrendStore = serde_yaml::from_reader(f).unwrap();

    let mut client = Client::connect(&conn_params, NoTls).unwrap();

    let result = load_trend_store(
        &mut client, &trend_store.data_source, &trend_store.entity_type, &trend_store.granularity
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
                        }, 
                        Err(e) => {
                            println!("Error applying update: {}", e);
                        }
                    }
                }
            } else {
                println!("Trend store already up-to-date")
            }
        },
        Err(e) => {
            println!("Error loading trend store: {}", e);
        }
    }
}

fn run_initialize_cmd(_matches: &ArgMatches, conn_params: &str) {
    let mut client = Client::connect(conn_params, NoTls).unwrap();
    let minerva_instance_root = env::var(ENV_MINERVA_INSTANCE_ROOT).unwrap();

    MinervaInstance::initialize_from(&mut client, &minerva_instance_root);
}

fn run_dump_cmd(_matches: &ArgMatches, conn_params: &str) {
    let mut client = Client::connect(conn_params, NoTls).unwrap();

    dump(&mut client);
}

fn run_diff_cmd(_matches: &ArgMatches, conn_params: &str) {
    let mut client = Client::connect(conn_params, NoTls).unwrap();

    let instance_db = match MinervaInstance::load_from_db(&mut client) {
        Ok(i) => i,
        Err(e) => {
            println!("Error loading instance from database: {}", e);
            return
        }
    };

    let minerva_instance_root = env::var(ENV_MINERVA_INSTANCE_ROOT).unwrap();

    let instance_def = MinervaInstance::load_from(&minerva_instance_root);

    let changes = instance_db.diff(&instance_def);

    if changes.len() > 0 {
        println!("Differences with database:");

        for change in changes {
            println!("{}", &change);
        }
    } else {
        println!("Database is up-to-date");
    }
}

fn run_update_cmd(_matches: &ArgMatches, conn_params: &str) {
    let mut client = Client::connect(conn_params, NoTls).unwrap();

    let instance_db = match MinervaInstance::load_from_db(&mut client) {
        Ok(i) => i,
        Err(e) => {
            println!("Error loading instance from database: {}", e);
            return
        }
    };

    let minerva_instance_root = env::var(ENV_MINERVA_INSTANCE_ROOT).unwrap();

    let instance_def = MinervaInstance::load_from(&minerva_instance_root);

    let result = instance_db.update(&mut client, &instance_def);

    match result {
        Ok(()) => {
            println!("Ok");
        },
        Err(e) => {
            println!("Error: {}", &e);
        }
    }
}