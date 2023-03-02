use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use tokio_postgres::GenericClient;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize)]
pub struct ParserConfig {
    pub entity_type: String,
    pub granularity: String,
    pub extra: Option<Value>,
}

pub async fn load_data<T: GenericClient + Send + Sync, P: AsRef<Path>>(
    client: &mut T,
    data_source: &str,
    parser_config: &ParserConfig,
    file_path: P,
) -> Result<(), String> {
    println!("Loading file {}", file_path.as_ref().to_string_lossy());

    let f = File::open(file_path).map_err(|e| format!("{}", e))?;

    let reader = BufReader::new(f);

    let mut csv_reader = csv::Reader::from_reader(reader);

    for result in csv_reader.records() {
        let record = result.unwrap();

        println!("{:?}", record);

        let entity: &str = record.get(0).unwrap();
        let timestamp: &str = record.get(1).unwrap();

        println!("Entity: {}", entity);
        println!("Timestamp: {}", timestamp);
    }

    Ok(())
}
