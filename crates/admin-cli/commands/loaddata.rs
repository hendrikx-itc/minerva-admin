use async_trait::async_trait;
use std::path::PathBuf;
use structopt::StructOpt;

use minerva::error::ConfigurationError;
use minerva::loading::{load_data, ParserConfig};

use super::common::{connect_db, Cmd, CmdResult};

#[derive(Debug, StructOpt)]
pub struct LoadDataOpt {
    #[structopt(long, help = "Data source of data")]
    data_source: Option<String>,
    #[structopt(long, parse(from_os_str), help = "File with parser configuration")]
    parser_config: Option<PathBuf>,
    #[structopt(parse(from_os_str), help = "File to load")]
    file: PathBuf,
}

#[async_trait]
impl Cmd for LoadDataOpt {
    async fn run(&self) -> CmdResult {
        let mut client = connect_db().await?;

        let parser_config: ParserConfig = match &self.parser_config {
            None => ParserConfig {
                entity_type: "node".into(),
                granularity: "15m".into(),
                extra: None,
            },
            Some(path) => {
                let config_file = std::fs::File::open(path).map_err(|e| ConfigurationError::from_msg(format!("{}", e)))?;
                serde_json::from_reader(config_file).unwrap()
            }
        };

        let data_source = match &self.data_source {
            None => "minerva-admin".to_string(),
            Some(d) => d.to_string(),
        };

        let result = load_data(&mut client, &data_source, &parser_config, &self.file).await;

        Ok(())
    }
}
