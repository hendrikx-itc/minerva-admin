use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use utoipa::ToSchema;

use bb8::Pool;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};

use actix_web::{delete, get, post, put, web::Data, web::Path, HttpResponse};

use serde_json::json;
use tokio_postgres::{GenericClient, types::Type};

use minerva::interval::parse_interval;

use crate::trendmaterialization::{
    TrendFunctionMaterializationData, TrendMaterializationFunctionData,
    TrendMaterializationSourceData, TrendMaterializationSourceIdentifier,
};
use crate::trendstore::{TrendData, TrendStorePartCompleteData};

use crate::error::{Error, Success};
use super::serviceerror::ServiceError;


lazy_static! {
    static ref DATASOURCE: String = "kpi".to_string();
    static ref DESCRIPTION: String = "".to_string();
    static ref GRANULARITIES: Vec<String> = vec![
        "15m".to_string(),
        "1h".to_string(),
        "1d".to_string(),
        "1w".to_string(),
        "1month".to_string()
    ];
    static ref LANGUAGE: String = "SQL".to_string();
    static ref TIME_AGGREGATION: String = "SUM".to_string();
    static ref ENTITY_AGGREGATION: String = "SUM".to_string();
    static ref MAPPING_FUNCTION: String = "trend.mapping_id".to_string();
    static ref DEFAULT_GRANULARITY: String = "1w".to_string();
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
    static ref PARTITION_SIZE: HashMap<String, Duration> = HashMap::from([
        ("15m".to_string(), parse_interval("1d").unwrap()),
        ("1h".to_string(), parse_interval("4d").unwrap()),
        ("1d".to_string(), parse_interval("3mons").unwrap()),
        ("1w".to_string(), parse_interval("1y").unwrap()),
        ("1month".to_string(), parse_interval("5y").unwrap())
    ]);
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct Kpi {
    pub trend_store_part: TrendStorePartCompleteData,
    pub materialization: TrendFunctionMaterializationData,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct TrendInfo {
    pub name: String,
    pub data_type: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct KpiRawData {
    pub name: String,
    pub entity_type: String,
    pub data_type: String,
    pub enabled: bool,
    pub source_trends: Vec<String>,
    pub definition: String,
    pub description: Value,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct KpiImplementedData {
    pub name: String,
    pub entity_type: String,
    pub data_type: String,
    pub enabled: bool,
    pub source_trendstore_parts: Vec<String>,
    pub definition: String,
    pub description: Value,
}

impl KpiRawData {
    async fn get_implemented_data<T: GenericClient + Send + Sync>(
        &self,
        client: &mut T,
    ) -> Result<KpiImplementedData, String> {
        let mut sources: Vec<String> = vec![];
        let mut result: Result<KpiImplementedData, String> = Err("Unexpected error".to_string());
        let mut errormet = false;

        let query = concat!(
            "SELECT tsp.name ",
            "FROM trend_directory.table_trend t ",
            "JOIN trend_directory.trend_store_part tsp ON t.trend_store_part_id = tsp.id ",
            "JOIN trend_directory.trend_store ts ON tsp.trend_store_id = ts.id ",
            "JOIN directory.entity_type et ON ts.entity_type_id = et.id ",
            "WHERE t.name = $1 AND ts.granularity = $2::interval AND et.name = $3"
        );

        for source_trend in self.source_trends.iter() {
            let statement = client
                .prepare_typed(query, &[Type::TEXT, Type::TEXT, Type::TEXT])
                .await
                .map_err(|e| format!("Could not prepare statement: {e}"))?;

            let query_result = client
                .query_one(
                    &statement,
                    &[source_trend, &*DEFAULT_GRANULARITY, &self.entity_type]
                )
                .await;

            match query_result {
                Err(_) => {
                    result = Err(format!(concat!(
                        "Could not find source trend store part for trend '{}': ",
                        "SELECT tsp.name ",
                        "FROM trend_directory.table_trend t ",
                        "JOIN trend_directory.trend_store_part tsp ON t.trend_store_part_id = tsp.id ",
                        "JOIN trend_directory.trend_store ts ON tsp.trend_store_id = ts.id ",
                        "JOIN directory.entity_type et ON ts.entity_type_id = et.id ",
                        "WHERE t.name = '{}' AND ts.granularity = '{}' AND et.name = '{}'"
                    ), source_trend, source_trend, *DEFAULT_GRANULARITY, self.entity_type));
                    errormet = true
                }
                Ok(row) => {
                    let tsp: String = row.get(0);
                    if !sources.contains(&tsp) {
                        sources.push(tsp.clone());
                    }
                }
            }
        }
        if errormet {
            result
        } else {
            Ok(KpiImplementedData {
                name: self.name.clone(),
                entity_type: self.entity_type.clone(),
                data_type: self.data_type.clone(),
                enabled: self.enabled,
                source_trendstore_parts: sources,
                definition: self.definition.clone(),
                description: self.description.clone(),
            })
        }
    }

    async fn get_kpi<T: GenericClient + Send + Sync>(
        &self,
        granularity: String,
        client: &mut T,
    ) -> Result<Kpi, String> {
        let query = self.get_implemented_data(client).await;
        match query {
            Err(e) => Err(e),
            Ok(implementedkpi) => Ok(implementedkpi.get_kpi(granularity)),
        }
    }

    async fn create<T: GenericClient + Send + Sync>(
        &self,
        client: &mut T,
    ) -> Result<String, Error> {
        let query = self.get_implemented_data(client).await;
        match query {
            Err(e) => Err(Error {
                code: 404,
                message: e,
            }),
            Ok(implementedkpi) => implementedkpi.create(client).await,
        }
    }
}

impl KpiImplementedData {
    fn target_trend_store_part(&self, granularity: String) -> String {
        format!(
            "{}-{}_{}_{}",
            *DATASOURCE,
            &self.name,
            &self.entity_type,
            granularity
        )
    }

    fn get_kpi(&self, granularity: String) -> Kpi {
        let mut sources: Vec<TrendMaterializationSourceData> = vec![];
        let mut modifieds: Vec<String> = vec![];
        let mut formats: Vec<String> = vec![];
        let mut partmodifieds: Vec<String> = vec![];
        let mut joins: Vec<String> = vec![];
        let mut i: i32 = 1;
        for tsp in self.source_trendstore_parts.iter() {
            let currenttsp =
                tsp.replace(&DEFAULT_GRANULARITY.to_string(), &granularity.to_string());
            sources.push(TrendMaterializationSourceData {
                trend_store_part: currenttsp.clone(),
                mapping_function: MAPPING_FUNCTION.to_string(),
            });
            modifieds.push(format!("modified{i}.last"));
            formats.push("\"%s\": \"%s\"".to_string());
            partmodifieds.push(format!("part{i}.name, modified{i}.last"));
            joins.push(format!(
		        "LEFT JOIN trend_directory.trend_store_part part{} ON part{}.name = '{}'\nJOIN trend_directory.modified modified{} ON modified{}.trend_store_part_id = part{}.id AND modified{}.timestamp = t.timestamp",
		        i, i, currenttsp.clone(), i, i, i, i
	        ));
            i += 1;
        }
        let fingerprint_function = format!("SELECT\n  greatest({}),\n  format('{}', {})::jsonb\nFROM (values($1)) as t(timestamp)\n{}",
					   modifieds.join(", "),
					   "{".to_owned() + &formats.join(", ") + "}",
					   partmodifieds.join(", "),
					   joins.join("\n")
					   );

        let mut sourcestrings: Vec<String> = vec![];
        let mut counter = 1;
        for source in &sources {
            match counter {
		1 => sourcestrings.push(format!("trend.\"{}\" t{}", source.trend_store_part, counter)),
		_ => sourcestrings.push(format!("trend.\"{}\" t{} ON t1.entity_id = t{}.entity_id and t1.timestamp = t{}.timestamp", source.trend_store_part, counter, counter, counter)),
	    };
            counter += 1
        }
        let srcdef = format!(
	    "SELECT t1.entity_id, $1 as timestamp, {} as \"{}\"\n FROM {}\nWHERE t1.timestamp = $1\nGROUP BY t1.entity_id",
	    self.definition, self.name, sourcestrings.join("\nJOIN ")
	);
        Kpi {
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
                sources,
                function: TrendMaterializationFunctionData {
                    return_type: format!(
                        "TABLE (entity_id integer, \"timestamp\" timestamptz, \"{}\" {})",
                        self.name, self.data_type
                    ),
                    src: srcdef,
                    language: LANGUAGE.clone(),
                },
                description: self.description.clone(),
                fingerprint_function,
            },
        }
    }

    async fn create<T: GenericClient + Send + Sync>(
        &self,
        client: &mut T,
    ) -> Result<String, Error> {
        let mut result: Result<String, Error> = Ok("KPI created".to_string());

        for granularity in GRANULARITIES.iter() {
            let kpi = self.get_kpi(granularity.to_string());
            let query_result = kpi.trend_store_part.create(client).await;
            match query_result {
                Ok(_) => {
                    let query_result = kpi.materialization.create(client).await;

                    if let Err(e) = query_result {
                        if result.is_ok() {
                            result = Err(Error {
                                code: e.code,
                                message: e.message,
                            })
                        }
                    }
                },
                Err(e) => {
                    if result.is_ok() {
                        result = Err(Error {
                            code: e.code,
                            message: e.message,
                        })
                    }
                },
            }
        }

        result
    }
}

#[utoipa::path(
    get,
    path="/kpis",
    responses(
	(status = 200, description = "List of existing KPIs", body = [KpiImplementedData]),
	(status = 500, description = "Database unreachable", body = Error),
    )
)]
#[get("/kpis")]
pub(super) async fn get_kpis(pool: Data<Pool<PostgresConnectionManager<NoTls>>>) -> Result<HttpResponse, ServiceError> {
    let client = pool.get().await.map_err(|_| ServiceError::PoolError)?;

    let sources: Vec<TrendMaterializationSourceIdentifier> = client
        .query(
            concat!(
                "SELECT materialization_id, tsp.name, timestamp_mapping_func::text ",
                "FROM trend_directory.materialization_trend_store_link ",
                "JOIN trend_directory.trend_store_part tsp ON trend_store_part_id = tsp.id"
            ),
            &[]
        )
        .await
        .map_err(|e|Error {
            code: 500,
            message: e.to_string(),
        })
        .map(|rows| rows
            .iter()
            .map(|row| TrendMaterializationSourceIdentifier {
                materialization: row.get(0),
                source: TrendMaterializationSourceData {
                    trend_store_part: row.get(1),
                    mapping_function: row.get(2),
                },
            })
            .collect()
        )?;

    let result: Vec<KpiImplementedData> = client
        .query(
            concat!(
                "SELECT t.name, et.name, t.data_type, m.enabled, m.id, routine_definition, m.description ",
                "FROM trend_directory.table_trend t ",
                "JOIN trend_directory.trend_store_part tsp ON t.trend_store_part_id = tsp.id ",
                "JOIN trend_directory.trend_store ts ON tsp.trend_store_id = ts.id ",
                "JOIN directory.data_source ds ON ts.data_source_id = ds.id ",
                "JOIN directory.entity_type et ON ts.entity_type_id = et.id ",
                "JOIN trend_directory.materialization m ON tsp.id = m.dst_trend_store_part_id ",
                "JOIN trend_directory.function_materialization fm ON m.id = fm.materialization_id ",
                "JOIN information_schema.routines ON FORMAT('%s.\"%s\"', routine_schema, routine_name) = fm.src_function ",
                "WHERE ds.name = $1 AND ts.granularity = $2::text::interval ",
                "ORDER BY t.name"
            ),
            &[&*DATASOURCE, &*DEFAULT_GRANULARITY]
        )
        .await
        .map_err(|e| Error {
            code: 500,
            message: e.to_string(),
        })
        .map(|rows| rows
            .iter()
            .map(|row| {
                let mat_id: i32 = row.get(4);
                let this_sources: Vec<String> = sources
                    .iter()
                    .filter(|source| source.materialization == mat_id)
                    .map(|source| source.source.trend_store_part.clone())
                    .collect();
             
                KpiImplementedData {
                    name: row.get(0),
                    entity_type: row.get(1),
                    data_type: row.get(2),
                    enabled: row.get(3),
                    source_trendstore_parts: this_sources,
                    definition: row.get(5),
                    description: row.get(6),
                }
            })
            .collect()
        )?;

    Ok(HttpResponse::Ok().json(result))
}

#[utoipa::path(
    get,
    path="/kpis/{name}",
    responses(
	(status = 200, description = "Content of KPI", body = [KpiImplementedData]),
	(status = 404, description = "KPI does not exist", body = Error),
	(status = 500, description = "Database unreachable", body = Error),
    )
)]
#[get("/kpis/{name}")]
pub(super) async fn get_kpi(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
    name: Path<String>,
) -> Result<HttpResponse, ServiceError> {
    let kpiname = name.into_inner().replace('_', " ");

    let client = pool.get().await.map_err(|_| ServiceError::PoolError)?;
    let kpi = client
        .query_one(
            concat!(
                "SELECT t.name, et.name, t.data_type, m.enabled, m.id, routine_definition, m.description ",
                "FROM trend_directory.table_trend t ",
                "JOIN trend_directory.trend_store_part tsp ON t.trend_store_part_id = tsp.id ",
                "JOIN trend_directory.trend_store ts ON tsp.trend_store_id = ts.id ",
                "JOIN directory.data_source ds ON ts.data_source_id = ds.id ",
                "JOIN directory.entity_type et ON ts.entity_type_id = et.id ",
                "JOIN trend_directory.materialization m ON tsp.id = m.dst_trend_store_part_id ",
                "JOIN trend_directory.function_materialization fm ON m.id = fm.materialization_id ",
                "JOIN information_schema.routines ON FORMAT('%s.\"%s\"', routine_schema, routine_name) = fm.src_function ",
                "WHERE ds.name = $1 AND ts.granularity = $2 AND t.name = $3"
            ),
            &[&*DATASOURCE, &*DEFAULT_GRANULARITY, &kpiname]
        )
        .await
        .map_err(|_| Error {
            code: 404,
            message: format!("KPI {} not found", &kpiname),
        })?;

    let materialization_id: i32 = kpi.get(4);
    let sources: Vec<String> = client
        .query(
            concat!(
                "SELECT materialization_id, tsp.name, timestamp_mapping_func::text ",
                "FROM trend_directory.materialization_trend_store_link ",
                "JOIN trend_directory.trend_store_part tsp ON trend_store_part_id = tsp.id ",
                "WHERE materialization_id = $1"
            ),
            &[&materialization_id]
        )
        .await
        .map_err(|e| Error {
            code: 500,
            message: e.to_string(),
        })
        .map(|rows| rows
            .iter()
            .map(|row| row.get(1))
            .collect()
        )?;

    let result = KpiImplementedData {
        name: kpi.get(0),
        entity_type: kpi.get(1),
        data_type: kpi.get(2),
        enabled: kpi.get(3),
        source_trendstore_parts: sources,
        definition: kpi.get(5),
        description: kpi.get(6),
    };

    Ok(HttpResponse::Ok().json(result))
}

// curl -H "Content-Type: application/json" -X POST -d '{"name":"average-output","entity_type":"Cell","data_type":"numeric","enabled":true,"source_trends":["L.Thrp.bits.UL.NsaDc","L.DL.CRS.RateAvg"],"definition":"public.safe_division(SUM(\"L.Thrp.bits.UL.NsaDc\"),SUM(\"L.DL.CRS.RateAvg\") * 1000)","description":{"type": "ratio", "numerator": [{"type": "trend", "value": "L.Thrp.bits.UL.NsaDC"}], "denominator": [{"type": "constant", "value": "1000"}, {"type": "operator", "value": "*"}, {"type": "trend", "value": "L.DL.CRS.RateAvg"}]}}' localhost:8000/kpis

#[utoipa::path(
    post,
    path="/kpis",
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
) -> Result<HttpResponse, ServiceError> {
    let data: KpiRawData = serde_json::from_str(&post)
        .map_err(|e| Error {
            code: 400,
            message: e.to_string(),
        })?;

    let mut client = pool.get().await.map_err(|_| ServiceError::PoolError)?;

    let mut transaction = client
        .transaction()
        .await
        .map_err(|e| Error {
            code: 500,
            message: e.to_string(),
        })?;

    let result = data.create(&mut transaction).await;

    if result.is_ok() {
        transaction
            .commit()
            .await
            .map_err(|e| Error {
                code: 500,
                message: e.to_string(),
            })?;
    };

    Ok(HttpResponse::Ok().json(Success {code: 200, message: "Successfully created KPI".to_string()}))
}

// curl -H "Content-Type: application/json" -X PUT -d '{"name":"average-output","entity_type":"Cell","data_type":"numeric","enabled":true,"source_trends":["L.Thrp.bits.UL.NsaDc"],"definition":"public.safe_division(SUM(\"L.Thrp.bits.UL.NsaDc\"),1000::numeric)","description":{"type": "ratio", "numerator": [{"type": "trend", "value": "L.Thrp.bits.UL.NsaDC"}], "denominator": [{"type": "constant", "value": "1000"}]}}' localhost:8000/kpis
#[utoipa::path(
    put,
    path="/kpis",
    responses(
	(status = 200, description = "Updated KPI", body = Success),
	(status = 400, description = "Input format incorrect", body = Error),
	(status = 404, description = "KPI not found", body = Error),
	(status = 409, description = "Update failed", body = Error),
	(status = 500, description = "General error", body = Error)
    )
)]
#[put("/kpis")]
pub(super) async fn update_kpi(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
    post: String,
) -> Result<HttpResponse, ServiceError> {
    let data: KpiRawData = serde_json::from_str(&post)
        .map_err(|e| Error {
            code: 400,
            message: e.to_string(),
        })?;

    let mut client = pool.get().await.map_err(|_| ServiceError::PoolError)?;

    let mut transaction = client
        .transaction()
        .await
        .map_err(|e|Error {
            code: 500,
            message: e.to_string(),
        })?;

    let mut result: Result<String, Error> = Ok("KPI changed".to_string());

    for granularity in GRANULARITIES.iter() {
        let kpiquery = data
            .get_kpi(granularity.to_string(), &mut transaction)
            .await;
        match kpiquery {
            Err(e) => {
                result = Err(Error {
                    code: 404,
                    message: e,
                })
            }
            Ok(kpi) => {
                let updatequery = kpi.materialization.update(&mut transaction);

                if let Err(e) = updatequery.await {
                    result = Err(e);
                }
            }
        }
    }

    if result.is_ok() {
        let commission = transaction.commit().await;
        if let Err(e) = commission {
            result = Err(Error {
                code: 500,
                message: e.to_string(),
            });
        }
    };

    match result {
        Ok(m) => Ok(HttpResponse::Ok().json(Success {
            code: 200,
            message: m,
        })),
        Err(Error {
            code: 404,
            message: e,
        }) => Err(Error {
            code: 404,
            message: e,
        }.into()),
        Err(Error {
            code: 409,
            message: e,
        }) => Err(Error {
            code: 409,
            message: e,
        }.into()),
        Err(Error {
            code: c,
            message: e,
        }) => Err(Error {
            code: c,
            message: e,
        }.into()),
    }
}

#[utoipa::path(
    delete,
    path="/kpis/{name}",
    responses(
	(status = 200, description = "Updated KPI", body = Success),
	(status = 400, description = "Input format incorrect", body = Error),
	(status = 404, description = "KPI not found", body = Error),
	(status = 409, description = "Update failed", body = Error),
	(status = 500, description = "General error", body = Error)
    )
)]
#[delete("/kpis/{name}")]
pub(super) async fn delete_kpi(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
    name: Path<String>,
) -> Result<HttpResponse, ServiceError> {
    let kpiname = name.into_inner().replace('_', " ");
    let mut client = pool.get().await.map_err(|_| ServiceError::PoolError)?;

    let mut transaction = client
        .transaction()
        .await
        .map_err(|e|Error {
            code: 500,
            message: e.to_string(),
        })?;


    let kpi = transaction
        .query_one(
            concat!(
                "SELECT t.name, et.name, t.data_type, m.enabled, m.id, routine_definition, m.description ",
                "FROM trend_directory.table_trend t ",
                "JOIN trend_directory.trend_store_part tsp ON t.trend_store_part_id = tsp.id ",
                "JOIN trend_directory.trend_store ts ON tsp.trend_store_id = ts.id ",
                "JOIN directory.data_source ds ON ts.data_source_id = ds.id ",
                "JOIN directory.entity_type et ON ts.entity_type_id = et.id ",
                "JOIN trend_directory.materialization m ON tsp.id = m.dst_trend_store_part_id ",
                "JOIN trend_directory.function_materialization fm ON m.id = fm.materialization_id ",
                "JOIN information_schema.routines ON FORMAT('%s.\"%s\"', routine_schema, routine_name) = fm.src_function ",
                "WHERE ds.name = $1 AND ts.granularity = $2 AND t.name = $3"
            ),
            &[&*DATASOURCE, &*DEFAULT_GRANULARITY, &kpiname]
        )
        .await
        .map_err(|_| Error {
            code: 404,
            message: format!("KPI {} not found", &kpiname),
        })?;

    let materialization_id: i32 = kpi.get(4);

    let sources: Vec<String> = transaction
        .query(
            concat!(
                "SELECT materialization_id, tsp.name, timestamp_mapping_func::text ",
                "FROM trend_directory.materialization_trend_store_link ",
                "JOIN trend_directory.trend_store_part tsp ON trend_store_part_id = tsp.id ",
                "WHERE materialization_id = $1"
            ),
            &[&materialization_id]
        )
        .await
        .map_err(|e| Error {
            code: 500,
            message: e.to_string(),
        })
        .map(|rows| rows
            .iter()
            .map(|row| row.get(1))
            .collect()
        )?;

    let kpidata = KpiImplementedData {
        name: kpi.get(0),
        entity_type: kpi.get(1),
        data_type: kpi.get(2),
        enabled: kpi.get(3),
        source_trendstore_parts: sources,
        definition: kpi.get(5),
        description: kpi.get(6),
    };

    let mut result: Result<String, Error> = Ok("KPI deleted".to_string());

    for granularity in GRANULARITIES.iter() {
        let kpi = kpidata.get_kpi(granularity.to_string());
        let deletionquery = kpi
            .materialization
            .as_minerva()
            .delete(&mut transaction)
            .await;

        if let Err(error) = deletionquery {
            result = Err(Error {
                code: 409,
                message: error.to_string(),
            });
        }
    }

    if result.is_ok() {
        let commission = transaction.commit().await;

        if let Err(e) = commission {
            result = Err(Error {
                code: 500,
                message: e.to_string(),
            });
        }
    }

    match result {
        Ok(m) => Ok(HttpResponse::Ok().json(Success {
            code: 200,
            message: m,
        })),
        Err(Error {
            code: 404,
            message: e,
        }) => Err(Error {
            code: 404,
            message: e,
        }.into()),
        Err(Error {
            code: 409,
            message: e,
        }) => Err(Error {
            code: 409,
            message: e,
        }.into()),
        Err(Error {
            code: c,
            message: e,
        }) => Err(Error {
            code: c,
            message: e,
        }.into()),
    }
}
