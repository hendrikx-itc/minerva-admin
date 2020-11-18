use clap::{crate_authors, crate_name, crate_version, App, Arg, SubCommand};
use postgres::{Client, NoTls};

mod minerva;

use minerva::trend_store::{TrendStore, create_trend_store, test_to_generated_trend};

fn main() {
    let trend_store_create_cmd = SubCommand::with_name("create")
        .about("create a trend store")
        .arg(Arg::with_name("definition"));

    let trend_store_cmd = SubCommand::with_name("trend-store")
        .about("manage trend stores")
        .arg(
            Arg::with_name("debug")
                .short("d")
                .help("print debug information verbosely"),
        )
        .subcommand(trend_store_create_cmd);

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
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("trend-store") {

        if let Some(matches) = matches.subcommand_matches("create") {
            let definition = matches.value_of("definition").unwrap();

            let f = std::fs::File::open(&definition).unwrap();
            let trend_store: TrendStore = serde_yaml::from_reader(f).unwrap();

            let mut client = Client::connect("host=localhost user=postgres dbname=minerva", NoTls).unwrap();

            let value: Option<i32> = create_trend_store(&mut client, &trend_store);

            println!("Created trend store with Id: {:?}", value);
            //let result = test_to_trend(&mut client, &trend_store.parts[0].trends[0]);

            //println!("Result: {:?}", &result);

            //let result = test_to_generated_trend(&mut client, &trend_store.parts[0].generated_trends[0]);

            //println!("Result: {:?}", &result);
        }
    }
}
