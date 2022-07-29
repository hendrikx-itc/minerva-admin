use std::time::Duration;

use bb8::Pool;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};

use actix_web::{get, web::Data, web::Path, HttpResponse, Responder};

use serde::{Deserialize, Serialize};

use utoipa::Component;

use minerva::interval::parse_interval;

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct TrendMaterializationSource {
    pub trend_store_part: i32,
    pub mapping_function: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
struct TrendMaterializationSourceIdentifier {
    materialization: i32,
    source: TrendMaterializationSource,
}

#[derive(Debug, Serialize, Deserialize, Clone, Component)]
pub struct TrendViewMaterialization {
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
    pub sources: Vec<TrendMaterializationSource>,
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
    pub sources: Vec<TrendMaterializationSource>,
    pub fingerprint_function: String,
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
        (status = 200, description = "List current trend view materialization items", body = [TrendViewMaterialization])
    )
)]
#[get("/trend-view-materializations")]
pub(super) async fn get_trend_view_materializations(
    pool: Data<Pool<PostgresConnectionManager<NoTls>>>,
) -> impl Responder {
    let mut m: Vec<TrendViewMaterialization> = vec![];
    let client = pool.get().await.unwrap();

    let mut sources: Vec<TrendMaterializationSourceIdentifier> = vec![];
    for row in client.query("SELECT materialization_id, trend_store_part_id, timestamp_mapping_func::text FROM trend_directory.materialization_trend_store_link", &[],).await.unwrap()
    {
	let source = TrendMaterializationSourceIdentifier {
	    materialization: row.get(0),
	    source: TrendMaterializationSource {
		trend_store_part: row.get(1),
		mapping_function: row.get(2),
	    },
	};
	sources.push(source)
    };

    for row in client.query("SELECT vm.id, m.id, pg_views.definition, dst_trend_store_part_id, processing_delay::text, stability_delay::text, reprocessing_period::text, enabled, pg_proc.prosrc FROM trend_directory.view_materialization vm JOIN trend_directory.materialization m ON vm.materialization_id = m.id JOIN pg_proc ON trend_directory.fingerprint_function_name(m) = proname JOIN pg_views ON schemaname = substring(src_view, '(.*?)\\.') AND viewname = substring(src_view, '\"(.*)\"')", &[],).await.unwrap()
    {
	let mat_id: i32 = row.get(1);

	let mut this_sources: Vec<TrendMaterializationSource> = vec![];
	for source in &sources
	{
	    if source.materialization == mat_id {
		this_sources.push(source.source.clone())
	    }
	}

	let materialization = TrendViewMaterialization {
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
	(status = 200, description = "Get a specific view materialization", body = TrendViewMaterialization),
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
            let mut sources: Vec<TrendMaterializationSource> = vec![];
            for inner_row in client.query("SELECT trend_store_part_id, timestamp_mapping_func::text FROM trend_directory.materialization_trend_store_link tsl JOIN trend_directory.view_materialization vm ON tsl.materialization_id = vm.materialization_id WHERE vm.id = $1", &[&vm_id],).await.unwrap()
	    {
		let source = TrendMaterializationSource {
		    trend_store_part: inner_row.get(0),
		    mapping_function: inner_row.get(1),
		};
		sources.push(source)
	    };

            let materialization = TrendViewMaterialization {
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
	    source: TrendMaterializationSource {
		trend_store_part: row.get(1),
		mapping_function: row.get(2),
	    },
	};
	sources.push(source)
    };

    for row in client.query("SELECT fm.id, m.id, dst_trend_store_part_id, processing_delay::text, stability_delay::text, reprocessing_period::text, pg_proc.prosrc FROM trend_directory.function_materialization fm JOIN trend_directory.materialization m ON fm.materialization_id = m.id JOIN pg_proc ON trend_directory.fingerprint_function_name(m) = proname", &[],).await.unwrap()
    {
	let mat_id: i32 = row.get(1);

	let mut this_sources: Vec<TrendMaterializationSource> = vec![];
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
            let mut sources: Vec<TrendMaterializationSource> = vec![];
            for inner_row in client.query("SELECT trend_store_part_id, timestamp_mapping_func::text FROM trend_directory.materialization_trend_store_link tsl JOIN trend_directory.view_materialization vm ON tsl.materialization_id = vm.materialization_id WHERE vm.id = $1", &[&fm_id],).await.unwrap()
	    {
		let source = TrendMaterializationSource {
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
