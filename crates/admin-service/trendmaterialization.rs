use std::time::Duration;

use bb8::Pool;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};

use actix_web::{get, post, web::Data, web::Path, HttpResponse, Responder};

use serde::{Deserialize, Serialize};
use utoipa::Component;

use minerva::interval::parse_interval;
use minerva::trend_materialization::{TrendViewMaterialization, TrendMaterialization, AddTrendMaterialization, TrendMaterializationSource};
use minerva::change::Change;

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct TrendMaterializationSourceData {
    pub trend_store_part: i32,
    pub mapping_function: String,
}

impl TrendMaterializationSourceData {
    fn as_minerva(&self) -> TrendMaterializationSource {
	TrendMaterializationSource {
	    trend_store_part: self.trend_store_part.to_string(),
	    mapping_function: self.mapping_function.to_string(),
	}
    }
}

fn as_minerva(sources: &Vec<TrendMaterializationSourceData>) ->  Vec<TrendMaterializationSource> {
    let mut result: Vec<TrendMaterializationSource> = vec![];
    for source in sources {
	result.push(source.as_minerva())
    };
    result
}

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
struct TrendMaterializationSourceIdentifier {
    materialization: i32,
    source: TrendMaterializationSourceData,
}

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct TrendViewMaterializationFull {
    pub id: i32,
    pub materialization_id: i32,
    pub target_trend_store_part: i32,
    pub enabled: bool,
    #[serde(with = "humantime_serde")]
    pub processing_delay: Duration,
    #[serde(with = "humantime_serde")]
    pub stability_delay: Duration,
    #[serde(with = "humantime_serde")]
    pub reprocessing_period: Duration,
    pub sources: Vec<TrendMaterializationSourceData>,
    pub view: String,
    pub fingerprint_function: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct TrendFunctionMaterialization {
    pub id: i32,
    pub materialization_id: i32,
    pub target_trend_store_part: i32,
    #[serde(with = "humantime_serde")]
    pub processing_delay: Duration,
    #[serde(with = "humantime_serde")]
    pub stability_delay: Duration,
    #[serde(with = "humantime_serde")]
    pub reprocessing_period: Duration,
    pub sources: Vec<TrendMaterializationSourceData>,
    pub fingerprint_function: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct TrendViewMaterializationData {
    pub target_trend_store_part: i32,
    pub enabled: bool,
    #[serde(with = "humantime_serde")]
    pub processing_delay: Duration,
    #[serde(with = "humantime_serde")]
    pub stability_delay: Duration,
    #[serde(with = "humantime_serde")]
    pub reprocessing_period: Duration,
    pub sources: Vec<TrendMaterializationSourceData>,
    pub view: String,
    pub fingerprint_function: String,
}

impl TrendViewMaterializationData {
    fn as_minerva(&self) -> TrendMaterialization {
	let sources = as_minerva(&(self.sources));
	TrendMaterialization::View(TrendViewMaterialization {
	    target_trend_store_part: self.target_trend_store_part.to_string(),
	    enabled: self.enabled,
	    processing_delay: self.processing_delay,
	    stability_delay: self.stability_delay,
	    reprocessing_period: self.reprocessing_period,
	    sources: sources,
	    view: self.view.to_string(),
	    fingerprint_function: self.fingerprint_function.to_string(),
	}
	)
    }
}

impl TrendViewMaterializationFull {
    fn data(&self) -> TrendViewMaterializationData {
	TrendViewMaterializationData {
	    target_trend_store_part: self.target_trend_store_part,
	    enabled: self.enabled,
	    processing_delay: self.processing_delay,
	    stability_delay: self.stability_delay,
	    reprocessing_period: self.reprocessing_period,
	    sources: self.sources.to_vec(),
	    view: self.view.to_string(),
	    fingerprint_function: self.fingerprint_function.to_string(),
	}
    }

    fn as_minerva(&self) -> TrendMaterialization {
	self.data().as_minerva()
    }
}

/// Get list of trend view materializations.
///
/// List trend view materializations from Minerva database.
///
/// One could call the api endpoint with following curl command.
/// ```text
/// curl localhost:8080/trend-view-materializations
/// ```
#[utoipa::path(
    responses(
        (status = 200, description = "List current trend view materialization items", body = [TrendViewMaterializationFull])
    )
)]
#[get("/trend-view-materializations")]
pub(super) async fn get_trend_view_materializations(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
) -> impl Responder {
    let mut m: Vec<TrendViewMaterializationFull> = vec![];
    let client = pool.get().await.unwrap();

    let mut sources: Vec<TrendMaterializationSourceIdentifier> = vec![];
    for row in client.query("SELECT materialization_id, trend_store_part_id, timestamp_mapping_func::text FROM trend_directory.materialization_trend_store_link", &[],).await.unwrap()
    {
	let source = TrendMaterializationSourceIdentifier {
	    materialization: row.get(0),
	    source: TrendMaterializationSourceData {
		trend_store_part: row.get(1),
		mapping_function: row.get(2),
	    },
	};
	sources.push(source)
    };

    for row in client.query("SELECT vm.id, m.id, pg_views.definition, dst_trend_store_part_id, processing_delay::text, stability_delay::text, reprocessing_period::text, enabled, pg_proc.prosrc FROM trend_directory.view_materialization vm JOIN trend_directory.materialization m ON vm.materialization_id = m.id JOIN pg_proc ON trend_directory.fingerprint_function_name(m) = proname JOIN pg_views ON schemaname = substring(src_view, '(.*?)\\.') AND viewname = substring(src_view, '\"(.*)\"')", &[],).await.unwrap()
    {
	let mat_id: i32 = row.get(1);

	let mut this_sources: Vec<TrendMaterializationSourceData> = vec![];
	for source in &sources
	{
	    if source.materialization == mat_id {
		this_sources.push(source.source.clone())
	    }
	}

	let materialization = TrendViewMaterializationFull {
	    id: row.get(0),
	    materialization_id: row.get(1),
	    target_trend_store_part: row.get(3),
	    enabled: row.get(7),
	    processing_delay: parse_interval(row.get(4)).unwrap(),
	    stability_delay: parse_interval(row.get(5)).unwrap(),
	    reprocessing_period: parse_interval(row.get(6)).unwrap(),
	    sources: this_sources,
	    view: row.get(2),
	    fingerprint_function: row.get(8)
	};

	m.push(materialization);
    }

    HttpResponse::Ok().json(m)
}

#[utoipa::path(
    responses(
	(status = 200, description = "Get a specific view materialization", body = TrendViewMaterializationFull),
	(status = 404, description = "View materialization not found", body = String)
    )
)]
#[get("/trend-view-materializations/{id}")]
pub(super) async fn get_trend_view_materialization(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
    id: Path<i32>,
) -> impl Responder {
    let vm_id = id.into_inner();

    let client = pool.get().await.unwrap();

    let query_result = client.query_one("SELECT vm.id, m.id, pg_views.definition, dst_trend_store_part_id, processing_delay::text, stability_delay::text, reprocessing_period::text, enabled, pg_proc.prosrc FROM trend_directory.view_materialization vm JOIN trend_directory.materialization m ON vm.materialization_id = m.id JOIN pg_proc ON trend_directory.fingerprint_function_name(m) = proname JOIN pg_views ON schemaname = substring(src_view, '(.*?)\\.') AND viewname = substring(src_view, '\"(.*)\"') WHERE vm.id = $1", &[&vm_id],).await;

    match query_result {
        Ok(row) => {
            let mut sources: Vec<TrendMaterializationSourceData> = vec![];
            for inner_row in client.query("SELECT trend_store_part_id, timestamp_mapping_func::text FROM trend_directory.materialization_trend_store_link tsl JOIN trend_directory.view_materialization vm ON tsl.materialization_id = vm.materialization_id WHERE vm.id = $1", &[&vm_id],).await.unwrap()
	    {
		let source = TrendMaterializationSourceData {
		    trend_store_part: inner_row.get(0),
		    mapping_function: inner_row.get(1),
		};
		sources.push(source)
	    };

            let materialization = TrendViewMaterializationFull {
                id: row.get(0),
                materialization_id: row.get(1),
                target_trend_store_part: row.get(3),
                enabled: row.get(7),
                processing_delay: parse_interval(row.get(4)).unwrap(),
                stability_delay: parse_interval(row.get(5)).unwrap(),
                reprocessing_period: parse_interval(row.get(6)).unwrap(),
                sources: sources,
                view: row.get(2),
                fingerprint_function: row.get(8),
            };

            HttpResponse::Ok().json(materialization)
        }
        Err(_e) => HttpResponse::NotFound().body(format!(
            "Trend view materialization with id {} not found",
            &vm_id
        )),
    }
}

#[utoipa::path(
    responses(
        (status = 200, description = "List current trend function materialization items", body = [TrendFunctionMaterialization])
    )
)]
#[get("/trend-function-materializations")]
pub(super) async fn get_trend_function_materializations(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
) -> impl Responder {
    let mut m: Vec<TrendFunctionMaterialization> = vec![];
    let client = pool.get().await.unwrap();

    let mut sources: Vec<TrendMaterializationSourceIdentifier> = vec![];
    for row in client.query("SELECT materialization_id, trend_store_part_id, timestamp_mapping_func::text FROM trend_directory.materialization_trend_store_link", &[],).await.unwrap()
    {
	let source = TrendMaterializationSourceIdentifier {
	    materialization: row.get(0),
	    source: TrendMaterializationSourceData {
		trend_store_part: row.get(1),
		mapping_function: row.get(2),
	    },
	};
	sources.push(source)
    };

    for row in client.query("SELECT fm.id, m.id, dst_trend_store_part_id, processing_delay::text, stability_delay::text, reprocessing_period::text, pg_proc.prosrc FROM trend_directory.function_materialization fm JOIN trend_directory.materialization m ON fm.materialization_id = m.id JOIN pg_proc ON trend_directory.fingerprint_function_name(m) = proname", &[],).await.unwrap()
    {
	let mat_id: i32 = row.get(1);

	let mut this_sources: Vec<TrendMaterializationSourceData> = vec![];
	for source in &sources
	{
	    if source.materialization == mat_id {
		this_sources.push(source.source.clone())
	    }
	}

	let materialization = TrendFunctionMaterialization {
	    id: row.get(0),
	    materialization_id: row.get(1),
	    target_trend_store_part: row.get(2),
	    processing_delay: parse_interval(row.get(3)).unwrap(),
	    stability_delay: parse_interval(row.get(4)).unwrap(),
	    reprocessing_period: parse_interval(row.get(5)).unwrap(),
	    sources: this_sources,
	    fingerprint_function: row.get(6)
	};

	m.push(materialization);
    }

    HttpResponse::Ok().json(m)
}

#[utoipa::path(
    responses(
	(status = 200, description = "Get a specific function materialization", body = TrendFunctionMaterialization),
	(status = 404, description = "View function not found", body = String)
    )
)]
#[get("/trend-function-materializations/{id}")]
pub(super) async fn get_trend_function_materialization(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
    id: Path<i32>,
) -> impl Responder {
    let fm_id = id.into_inner();

    let client = pool.get().await.unwrap();

    let query_result = client.query_one("SELECT fm.id, m.id, dst_trend_store_part_id, processing_delay::text, stability_delay::text, reprocessing_period::text, pg_proc.prosrc FROM trend_directory.function_materialization fm JOIN trend_directory.materialization m ON fm.materialization_id = m.id JOIN pg_proc ON trend_directory.fingerprint_function_name(m) = proname WHERE fm.id = $1", &[&fm_id],).await;

    match query_result {
        Ok(row) => {
            let mut sources: Vec<TrendMaterializationSourceData> = vec![];
            for inner_row in client.query("SELECT trend_store_part_id, timestamp_mapping_func::text FROM trend_directory.materialization_trend_store_link tsl JOIN trend_directory.view_materialization vm ON tsl.materialization_id = vm.materialization_id WHERE vm.id = $1", &[&fm_id],).await.unwrap()
	    {
		let source = TrendMaterializationSourceData {
		    trend_store_part: inner_row.get(0),
		    mapping_function: inner_row.get(1),
		};
		sources.push(source)
	    };

            let materialization = TrendFunctionMaterialization {
                id: row.get(0),
                materialization_id: row.get(1),
                target_trend_store_part: row.get(2),
                processing_delay: parse_interval(row.get(3)).unwrap(),
                stability_delay: parse_interval(row.get(4)).unwrap(),
                reprocessing_period: parse_interval(row.get(5)).unwrap(),
                sources: sources,
                fingerprint_function: row.get(6),
            };

            HttpResponse::Ok().json(materialization)
        }
        Err(_e) => HttpResponse::NotFound().body(format!(
            "Trend function materialization with id {} not found",
            &fm_id
        )),
    }
}

// To call this with curl:
// first: DROP TABLE trend."u2020-4g-pm_v-site_lcs_1w_staging";
//        DELETE FROM trend_directory.materialization;
// curl -H "Content-Type: application/json" -X POST -d '{"target_trend_store_part":668,"enabled":true,"processing_delay":"30m","stability_delay":"5m","reprocessing_period":"3days","sources":[{"trend_store_part": 5, "mapping_function": "trend.mapping_id(timestamp with time zone)"}, {"trend_store_part": 6, "mapping_function": "trend.mapping_id(timestamp with time zone)"}],"view":"SELECT r.target_id AS entity_id, t.\"timestamp\", sum(t.samples) AS samples, sum(t.\"L.LCS.EcidMeas.Req\") AS \"L.LCS.EcidMeas.Req\", sum(t.\"L.LCS.EcidMeas.Succ\") AS \"L.LCS.EcidMeas.Succ\", sum(t.\"L.LCS.OTDOAInterFreqRSTDMeas.Succ\") AS \"L.LCS.OTDOAInterFreqRSTDMeas.Succ\" FROM (trend.\"u2020-4g-pm_Cell_lcs_1w\" t JOIN relation.\"Cell->v-site\" r ON (t.entity_id = r.source_id)) GROUP BY t.\"timestamp\", r.target_id;", "fingerprint_function":"SELECT modified.last, '\''{}'\''::jsonb FROM trend_directory.modified JOIN trend_directory.trend_store_part ttsp ON ttsp.id = modified.trend_store_part_id WHERE modified.timestamp = $1;"}' localhost:8080/trend-view-materializations/new

#[utoipa::path(
    responses(
	(status = 200, description = "Create a new view materialization", body = TrendFunctionMaterialization),
	(status = 400, description = "Missing or incorrect data", body = String),
    )
)]
#[post("/trend-view-materializations/new")]
pub(super) async fn post_trend_view_materialization(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
    post: String,
) -> impl Responder {
    let input: Result<TrendViewMaterializationData, serde_json::Error> = serde_json::from_str(&post);
    match input {
	Err(e) => HttpResponse::BadRequest().body(e.to_string()),
	Ok(data) => {
	    let mut client = pool.get().await.unwrap();
	    let action = AddTrendMaterialization {
		trend_materialization: data.as_minerva(),
	    };
	    let result = action.apply(&mut client).await;
	    match result {
		Err(e) => HttpResponse::Conflict().body(e.to_string()),
		Ok(_) => {
		    let query_result = client.query_one("SELECT vm.id, m.id, pg_views.definition, dst_trend_store_part_id, processing_delay::text, stability_delay::text, reprocessing_period::text, enabled, pg_proc.prosrc FROM trend_directory.view_materialization vm JOIN trend_directory.materialization m ON vm.materialization_id = m.id JOIN pg_proc ON trend_directory.fingerprint_function_name(m) = proname JOIN pg_views ON schemaname = substring(src_view, '(.*?)\\.') AND viewname = substring(src_view, '\"(.*)\"') WHERE dst_trend_store_part_id = $1", &[&data.target_trend_store_part],).await;
		    match query_result {
			Err(e) => HttpResponse::NotFound().body("Creation reported as succeeded, but could not find created view materialization afterward: ".to_owned() + &e.to_string()),
			Ok(row) => {
			    let mut sources: Vec<TrendMaterializationSourceData> = vec![];
			    let id: i32 = row.get(0);
			    for inner_row in client.query("SELECT trend_store_part_id, timestamp_mapping_func::text FROM trend_directory.materialization_trend_store_link tsl JOIN trend_directory.view_materialization vm ON tsl.materialization_id = vm.materialization_id WHERE vm.id = $1", &[&id],).await.unwrap()
			    {
				let source = TrendMaterializationSourceData {
				    trend_store_part
					: inner_row.get(0),
				    mapping_function: inner_row.get(1),
				};
				sources.push(source)
			    };
			    
			    let materialization = TrendViewMaterializationFull {
				id: id,
				materialization_id: row.get(1),
				target_trend_store_part: row.get(3),
				enabled: row.get(7),
				processing_delay: parse_interval(row.get(4)).unwrap(),
				stability_delay: parse_interval(row.get(5)).unwrap(),
				reprocessing_period: parse_interval(row.get(6)).unwrap(),
				sources: sources,
				view: row.get(2),
				fingerprint_function: row.get(8),
			    };

			    HttpResponse::Ok().json(materialization)
			}
		    }
		}
	    }
	}
    }
}


