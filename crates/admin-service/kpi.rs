use serde::{Deserialize, Serialize};
use utoipa::Component;
use std::collections::HashMap;
use std::time::Duration;
use lazy_static::lazy_static;

use serde_json::json;
use tokio_postgres::Client;

use minerva::interval::parse_interval;

use crate::trendstore::{TrendData, TrendStorePartData};
use crate::trendmaterialization::{TrendFunctionMaterializationData, TrendMaterializationFunctionData, TrendMaterializationSourceData};

lazy_static! {
    static ref DATASOURCE: String = "kpi".to_string();
    static ref DESCRIPTION: String = "".to_string();
    static ref GRANULARITIES: Vec<String> = vec![ "15m".to_string(), "1h".to_string(), "1d".to_string(), "1w".to_string(), "1month".to_string() ];
    static ref LANGUAGE: String = "SQL".to_string();
    static ref TIME_AGGREGATION: String = "SUM".to_string();
    static ref ENTITY_AGGREGATION: String = "SUM".to_string();
    static ref MAPPING_FUNCTION: String = "trend.mapping_id(timestamp with time zone)".to_string();
    static ref PROCESSING_DELAY: HashMap<String, Duration> = HashMap::from([
	("15m".to_string(), parse_interval("10m").unwrap()),
	("1h".to_string(), parse_interval("10m").unwrap()),
	("1d".to_string(), parse_interval("30m").unwrap()),
	("1w".to_string(), parse_interval("30m").unwrap()),
	("1month".to_string(), parse_interval("30m").unwrap()),
    ]);
    static ref STABILITY_DELAY: HashMap<String, Duration> = HashMap::from([
	("15m".to_string(), parse_interval("5m").unwrap()),
	("1h".to_string(), parse_interval("5m").unwrap()),
	("1d".to_string(), parse_interval("5m").unwrap()),
	("1w".to_string(), parse_interval("5m").unwrap()),
	("1month".to_string(), parse_interval("5m").unwrap()),
    ]);
    static ref REPROCESSING_PERIOD: HashMap<String, Duration> = HashMap::from([
	("15m".to_string(), parse_interval("3 days").unwrap()),
	("1h".to_string(), parse_interval("3 days").unwrap()),
	("1d".to_string(), parse_interval("3 days").unwrap()),
	("1w".to_string(), parse_interval("3 days").unwrap()),
	("1month".to_string(), parse_interval("3 days").unwrap())
    ]);
}


#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct Kpi {
    pub trend_store_part: TrendStorePartData,
    pub materialization: TrendFunctionMaterializationData,
}

pub struct TrendInfo {
    pub name: String,
    pub data_type: String,
}

pub struct KpiData {
    pub name: String,
    pub entity_type: String,
    pub data_type: String,
    pub enabled: bool,
    pub source_trends: Vec<String>,
    pub definition: String,
}

impl KpiData {

    fn target_trend_store_part(&self, granularity: String) -> String {
	format!("{}-{}-_{}_{}", DATASOURCE.to_string(), &self.name, &self.entity_type, granularity)
    }

    async fn get_kpi(&self, granularity: String, client: &mut Client) -> Result<Kpi, String> {

	let mut problem = "".to_string();
	let mut sources: Vec<TrendMaterializationSourceData> = vec![];

	let mut modifieds: Vec<String> = vec![];
	let mut formats: Vec<String> = vec![];
	let mut partmodifieds: Vec<String> = vec![];
	let mut joins: Vec<String> = vec![];
	let mut i: i32 = 1;
	
	
	for source_trend in self.source_trends.iter() {
	    let query_result = client.query_one("SELECT tsp.name FROM trend_directory.table_trend t JOIN trend_directory.trend_store_part tsp ON t.trend_store_part_id = tsp.id JOIN directory.entity_type et ON tsp.entity_type = et.id WHERE t.name = $1 AND et.name = $2", &[&source_trend, &self.entity_type],).await;
	    match query_result {
		Err(_) => {
		    problem = format!("trend {} not found or not unique", source_trend)
		},
		Ok(row) => {
		    let tsp: String = row.get(0);
		    sources.push(TrendMaterializationSourceData {
			trend_store_part: tsp.clone(),
			mapping_function: MAPPING_FUNCTION.to_string(),
		    });
		    modifieds.push(format!("modified{}.last", i.to_string()));
		    formats.push("{\"%s\": \"%s\"}".to_string());
		    partmodifieds.push(format!("part{}.name, modified{}.last", i.to_string(), i.to_string()));
		    joins.push(format!("LEFT JOIN trend_directory.trend_store_part part{} ON part{}.name = '{}'\nJOIN trend_directory.modified modified{} ON modified{}.trend_store_part_id = part{}a.id AND modified{}.timestamp = t.timestamp", i.to_string(), i.to_string(), tsp.clone(), i.to_string(), i.to_string(), i.to_string(), i.to_string()));
		    i = i+1;
		}
	    }
	};
	let fingerprint_function = format!("SELECT\n  greatest({}),\n  format('{}', {})::jsonb\nFROM (values($1)) as t(timestamp)\n{}",
					   modifieds.join(", "),
					   formats.join(", "),
					   partmodifieds.join(", "),
					   joins.join("\n")
					   );
					   
	match problem.as_str() {
	    "" => {
		Ok(Kpi {
		    trend_store_part: TrendStorePartData {
			name: self.target_trend_store_part(granularity.clone()),
			trends: vec![
			    TrendData {
				name: self.name.clone(),
				data_type: self.data_type.clone(),
				time_aggregation: TIME_AGGREGATION.clone(),
				entity_aggregation: ENTITY_AGGREGATION.clone(),
				extra_data: json!("{}"),
				description: DESCRIPTION.clone(),
			    }
			],
			generated_trends: vec![],
		    },
		    materialization: TrendFunctionMaterializationData {
			enabled: self.enabled,
			target_trend_store_part: self.target_trend_store_part(granularity.clone()),
			processing_delay: PROCESSING_DELAY.get(granularity.as_str()).unwrap().to_owned(),
			stability_delay: STABILITY_DELAY.get(granularity.as_str()).unwrap().to_owned(),
			reprocessing_period: REPROCESSING_PERIOD.get(granularity.as_str()).unwrap().to_owned(),
			sources: Vec::new(), // to correct
			function: TrendMaterializationFunctionData {
			    return_type: format!(
				"TABLE entity_id integer, timestamp, {} {})",
				self.name, self.data_type
			    ),
			    src: self.definition.clone(),
			    language: LANGUAGE.clone()
			},
			fingerprint_function: fingerprint_function.to_string()
		    }
		})
	    },
	    error => Err(error.to_string())
	}
    }
}
