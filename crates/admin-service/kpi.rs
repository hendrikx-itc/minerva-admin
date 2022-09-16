use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use utoipa::Component;

use bb8::Pool;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};

use actix_web::{get, post, web::Data, HttpResponse, Responder};

use serde_json::json;
use tokio_postgres::Client;

use minerva::interval::parse_interval;

use crate::trendmaterialization::{
    TrendFunctionMaterializationData, TrendMaterializationFunctionData,
    TrendMaterializationSourceData, TrendMaterializationSourceIdentifier,
};
use crate::trendstore::{TrendData, TrendStorePartCompleteData};

use crate::error::{Error, Success};

lazy_static! {
    static ref DATASOURCE: String = "kpi".to_string();
    static ref DESCRIPTION: String = "".to_string();
    static ref GRANULARITIES: Vec<String> = vec![
        "15m".to_string(),
        "1h".to_string(),
        "1d".to_string(),
        "1w".to_string(),
        "1mon".to_string()
    ];
    static ref LANGUAGE: String = "SQL".to_string();
    static ref TIME_AGGREGATION: String = "SUM".to_string();
    static ref ENTITY_AGGREGATION: String = "SUM".to_string();
    static ref MAPPING_FUNCTION: String = "trend.mapping_id(timestamptz)".to_string();
    static ref DEFAULT_GRANULARITY: String = "15m".to_string();
    static ref PROCESSING_DELAY: HashMap<String, Duration> = HashMap::from([
        ("15m".to_string(), parse_interval("10m").unwrap()),
        ("1h".to_string(), parse_interval("10m").unwrap()),
        ("1d".to_string(), parse_interval("30m").unwrap()),
        ("1w".to_string(), parse_interval("30m").unwrap()),
        ("1mon".to_string(), parse_interval("30m").unwrap()),
    ]);
    static ref STABILITY_DELAY: HashMap<String, Duration> = HashMap::from([
        ("15m".to_string(), parse_interval("5m").unwrap()),
        ("1h".to_string(), parse_interval("5m").unwrap()),
        ("1d".to_string(), parse_interval("5m").unwrap()),
        ("1w".to_string(), parse_interval("5m").unwrap()),
        ("1mon".to_string(), parse_interval("5m").unwrap()),
    ]);
    static ref REPROCESSING_PERIOD: HashMap<String, Duration> = HashMap::from([
        ("15m".to_string(), parse_interval("3 days").unwrap()),
        ("1h".to_string(), parse_interval("3 days").unwrap()),
        ("1d".to_string(), parse_interval("3 days").unwrap()),
        ("1w".to_string(), parse_interval("3 days").unwrap()),
        ("1mon".to_string(), parse_interval("3 days").unwrap())
    ]);
    static ref PARTITION_SIZE: HashMap<String, Duration> = HashMap::from([
        ("15m".to_string(), parse_interval("1d").unwrap()),
        ("1h".to_string(), parse_interval("4d").unwrap()),
        ("1d".to_string(), parse_interval("3mons").unwrap()),
        ("1w".to_string(), parse_interval("1y").unwrap()),
        ("1mon".to_string(), parse_interval("5y").unwrap())
    ]);
}

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct Kpi {
    pub trend_store_part: TrendStorePartCompleteData,
    pub materialization: TrendFunctionMaterializationData,
}

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct TrendInfo {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
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
        format!(
            "{}-{}_{}_{}",
            DATASOURCE.to_string(),
            &self.name,
            &self.entity_type,
            granularity
        )
    }

    async fn get_kpi(&self, granularity: String, client: &mut Client) -> Result<Kpi, String> {
        let mut problem = "".to_string();
        let mut sources: Vec<TrendMaterializationSourceData> = vec![];
        let mut foundsources: Vec<String> = vec![];

        let mut modifieds: Vec<String> = vec![];
        let mut formats: Vec<String> = vec![];
        let mut partmodifieds: Vec<String> = vec![];
        let mut joins: Vec<String> = vec![];
        let mut i: i32 = 1;

        for source_trend in self.source_trends.iter() {
            let query_result = client.query_one(&format!("SELECT tsp.name FROM trend_directory.table_trend t JOIN trend_directory.trend_store_part tsp ON t.trend_store_part_id = tsp.id JOIN trend_directory.trend_store ts ON tsp.trend_store_id = ts.id JOIN directory.entity_type et ON ts.entity_type_id = et.id WHERE t.name = '{}' AND ts.granularity = '{}' AND et.name = '{}'", source_trend, granularity, self.entity_type), &[],).await;
            match query_result {
                Err(_) => {
                    problem = format!(
                        "Trend {} not found or not unique for entity type {}",
                        source_trend, self.entity_type
                    )
                }
                Ok(row) => {
                    let tsp: String = row.get(0);
                    if !foundsources.contains(&tsp) {
                        foundsources.push(tsp.clone());
                        sources.push(TrendMaterializationSourceData {
                            trend_store_part: tsp.clone(),
                            mapping_function: MAPPING_FUNCTION.to_string(),
                        });
                        modifieds.push(format!("modified{}.last", i.to_string()));
                        formats.push("{\"%s\": \"%s\"}".to_string());
                        partmodifieds.push(format!(
                            "part{}.name, modified{}.last",
                            i.to_string(),
                            i.to_string()
                        ));
                        joins.push(format!(
			    "LEFT JOIN trend_directory.trend_store_part part{} ON part{}.name = '{}'\nJOIN trend_directory.modified modified{} ON modified{}.trend_store_part_id = part{}.id AND modified{}.timestamp = t.timestamp",
			    i.to_string(), i.to_string(), tsp.clone(), i.to_string(), i.to_string(), i.to_string(), i.to_string()
			));
                        i = i + 1;
                    }
                }
            }
        }
        let fingerprint_function = format!("SELECT\n  greatest({}),\n  format('{}', {})::jsonb\nFROM (values($1)) as t(timestamp)\n{}",
					   modifieds.join(", "),
					   formats.join(", "),
					   partmodifieds.join(", "),
					   joins.join("\n")
					   );

        match problem.as_str() {
            "" => {
                let mut sourcestrings: Vec<String> = vec![];
                let mut counter = 1;
                for source in &sources {
                    match counter {
			1 => sourcestrings.push(format!("trend.\"{}\" t{}", source.trend_store_part, counter)),
			_ => sourcestrings.push(format!("trend.\"{}\" t{} ON t1.entity_id = t{}.entity_id and t1.timestamp = t{}.timestamp", source.trend_store_part, counter, counter, counter)),
		    };
                    counter = counter + 1
                }
                let srcdef = format!(
		    "SELECT t1.entity_id, $1 as timestamp, {} as \"{}\"\n FROM {}\nWHERE t1.timestamp = $1\nGROUP BY t1.entity_id",
		    self.definition, self.name, sourcestrings.join("\nJOIN ")
		);
                Ok(Kpi {
                    trend_store_part: TrendStorePartCompleteData {
                        name: self.target_trend_store_part(granularity.clone()),
                        entity_type: self.entity_type.clone(),
                        data_source: DATASOURCE.to_string(),
                        granularity: parse_interval(&granularity).unwrap(),
                        partition_size: *PARTITION_SIZE.get(granularity.as_str()).unwrap(),
                        trends: vec![TrendData {
                            name: self.name.clone(),
                            data_type: self.data_type.clone(),
                            time_aggregation: TIME_AGGREGATION.clone(),
                            entity_aggregation: ENTITY_AGGREGATION.clone(),
                            extra_data: json!("{}"),
                            description: DESCRIPTION.clone(),
                        }],
                        generated_trends: vec![],
                    },
                    materialization: TrendFunctionMaterializationData {
                        enabled: self.enabled,
                        target_trend_store_part: self.target_trend_store_part(granularity.clone()),
                        processing_delay: PROCESSING_DELAY
                            .get(granularity.as_str())
                            .unwrap()
                            .to_owned(),
                        stability_delay: STABILITY_DELAY
                            .get(granularity.as_str())
                            .unwrap()
                            .to_owned(),
                        reprocessing_period: REPROCESSING_PERIOD
                            .get(granularity.as_str())
                            .unwrap()
                            .to_owned(),
                        sources: sources,
                        function: TrendMaterializationFunctionData {
                            return_type: format!(
                                "TABLE (entity_id integer, \"timestamp\" timestamptz, \"{}\" {})",
                                self.name, self.data_type
                            ),
                            src: srcdef.clone(),
                            language: LANGUAGE.clone(),
                        },
                        fingerprint_function: fingerprint_function.to_string(),
                    },
                })
            }
            error => Err(error.to_string()),
        }
    }

    async fn create(&self, client: &mut Client) -> Result<String, Error> {
        let mut result: Result<String, Error> = Ok("KPI created".to_string());
        for granularity in GRANULARITIES.iter() {
            let query_result = self.get_kpi(granularity.to_string(), client).await;
            match query_result {
                Err(e) => {
                    result = Err(Error {
                        code: 409,
                        message: format!("Preparation failed: {}", e.to_string()),
                    })
                }
                Ok(kpi) => {
                    let query_result = kpi.trend_store_part.create(client).await;
                    match query_result {
                        Ok(_) => {
                            let query_result = kpi.materialization.create(client).await;
                            match query_result {
                                Err(e) => result = Err(e),
                                Ok(_) => {}
                            }
                        }
                        Err(e) => result = Err(e),
                    }
                }
            }
        }
        result
    }
}

#[utoipa::path(
    responses(
	(status = 200, description = "List of existing KPIs", body = [KpiData]),
	(status = 500, description = "Database unreachable", body = Error),
    )
)]
#[get("/kpis")]
pub(super) async fn get_kpis(pool: Data<Pool<PostgresConnectionManager<NoTls>>>) -> impl Responder {
    let mut result: Vec<KpiData> = vec![];

    let clientquery = pool.get().await;
    match clientquery {
        Err(e) => HttpResponse::InternalServerError().json(Error {
            code: 500,
            message: e.to_string(),
        }),
        Ok(client) => {
            let mut sources: Vec<TrendMaterializationSourceIdentifier> = vec![];
            let query = client.query("SELECT materialization_id, tsp.name, timestamp_mapping_func::text FROM trend_directory.materialization_trend_store_link JOIN trend_directory.trend_store_part tsp ON trend_store_part_id = tsp.id", &[],).await;
            match query {
                Err(e) => HttpResponse::InternalServerError().json(Error {
                    code: 500,
                    message: e.to_string(),
                }),
                Ok(query_result) => {
                    for row in query_result {
                        let source = TrendMaterializationSourceIdentifier {
                            materialization: row.get(0),
                            source: TrendMaterializationSourceData {
                                trend_store_part: row.get(1),
                                mapping_function: row.get(2),
                            },
                        };
                        sources.push(source)
                    }

                    let query = client.query(&format!("SELECT t.name, et.name, t.data_type, m.enabled, m.id, routine_definition FROM trend_directory.table_trend t JOIN trend_directory.trend_store_part tsp ON t.trend_store_part_id = tsp.id JOIN trend_directory.trend_store ts ON tsp.trend_store_id = ts.id JOIN directory.data_source ds ON ts.data_source_id = ds.id JOIN directory.entity_type et ON ts.entity_type_id = et.id JOIN trend_directory.materialization m ON tsp.id = m.dst_trend_store_part_id JOIN trend_directory.function_materialization fm ON m.id = fm.materialization_id JOIN information_schema.routines ON FORMAT('%s.\"%s\"', routine_schema, routine_name) = fm.src_function WHERE ds.name = '{}' AND ts.granularity = '{}'", DATASOURCE.to_string(), DEFAULT_GRANULARITY.to_string()), &[]).await;
                    match query {
                        Err(e) => HttpResponse::InternalServerError().json(Error {
                            code: 500,
                            message: e.to_string(),
                        }),
                        Ok(query_result) => {
                            for row in query_result {
                                let mat_id: i32 = row.get(4);
                                let mut this_sources: Vec<String> = vec![];
                                for source in &sources {
                                    if source.materialization == mat_id {
                                        this_sources.push(source.source.trend_store_part.clone())
                                    }
                                }

                                let kpi = KpiData {
                                    name: row.get(0),
                                    entity_type: row.get(1),
                                    data_type: row.get(2),
                                    enabled: row.get(3),
                                    source_trends: this_sources,
                                    definition: row.get(5),
                                };
                                result.push(kpi)
                            }
                            HttpResponse::Ok().json(result)
                        }
                    }
                }
            }
        }
    }
}

// curl -H "Content-Type: application/json" -X POST -d '{"name":"average-output","entity_type":"Cell","data_type":"numeric","enabled":true,"source_trends":["L.Thrp.bits.UL.NsaDc","L.DL.CRS.RateAvg"],"definition":"public.safe_division(SUM(\"L.Thrp.bits.UL.NsaDc\"),SUM(\"L.DL.CRS.RateAvg\") * 1000)"}' localhost:8080/kpis

#[utoipa::path(
    responses(
	(status = 200, description = "Create a new KPI", body = Success),
	(status = 400, description = "Incorrect data format", body = Error),
	(status = 409, description = "KPI creation failed", body = Error),
	(status = 500, description = "Database unreachable", body = Error),
    )
)]
#[post("/kpis")]
pub(super) async fn post_kpi(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
    post: String,
) -> impl Responder {
    let input: Result<KpiData, serde_json::Error> = serde_json::from_str(&post);
    match input {
        Err(e) => HttpResponse::BadRequest().json(Error {
            code: 400,
            message: e.to_string(),
        }),
        Ok(data) => {
            let client_query = pool.get().await;
            match client_query {
                Err(e) => HttpResponse::InternalServerError().json(Error {
                    code: 500,
                    message: e.to_string(),
                }),
                Ok(mut client) => {
                    let result = data.create(&mut client).await;
                    match result {
                        Ok(e) => HttpResponse::Ok().json(Success {
                            code: 200,
                            message: e,
                        }),
                        Err(Error {
                            code: 409,
                            message: e,
                        }) => HttpResponse::Conflict().json(Error {
                            code: 409,
                            message: e,
                        }),
                        Err(Error {
                            code: c,
                            message: e,
                        }) => HttpResponse::InternalServerError().json(Error {
                            code: c,
                            message: e,
                        }),
                    }
                }
            }
        }
    }
}
