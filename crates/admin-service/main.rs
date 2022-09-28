use actix_cors::Cors;
use actix_web::{middleware::Logger, web, App, HttpServer};

use bb8;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};

use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

mod trendmaterialization;
use trendmaterialization::{
    delete_trend_function_materialization, delete_trend_view_materialization,
    get_trend_function_materialization, get_trend_function_materializations,
    get_trend_materializations, get_trend_view_materialization, get_trend_view_materializations,
    post_trend_function_materialization, post_trend_view_materialization,
    update_trend_function_materialization, update_trend_view_materialization,
    TrendFunctionMaterializationData, TrendFunctionMaterializationFull, TrendMaterializationDef,
    TrendMaterializationSourceData, TrendViewMaterializationData, TrendViewMaterializationFull,
};

mod trendstore;
use trendstore::{
    find_trend_store_part, get_trend_store, get_trend_store_part, get_trend_store_parts,
    get_trend_stores, post_trend_store_part, GeneratedTrendFull, TrendFull, TrendStoreFull,
    TrendStorePartFull,
};

mod datasource;
use datasource::{get_data_source, get_data_sources, DataSource};

mod entitytype;
use entitytype::{get_entity_type, get_entity_types, EntityType};

mod kpi;
use kpi::{get_kpis, post_kpi, update_kpi, KpiData};

mod error;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    #[derive(OpenApi)]
    #[openapi(
        handlers(
            trendmaterialization::get_trend_view_materializations,
	    trendmaterialization::get_trend_view_materialization,
            trendmaterialization::get_trend_function_materializations,
	    trendmaterialization::get_trend_function_materialization,
	    trendmaterialization::get_trend_materializations,
	    trendmaterialization::post_trend_view_materialization,
	    trendmaterialization::post_trend_function_materialization,
	    trendmaterialization::delete_trend_view_materialization,
	    trendmaterialization::delete_trend_function_materialization,
	    trendmaterialization::update_trend_function_materialization,
	    trendmaterialization::update_trend_view_materialization,
	    trendstore::get_trend_store_parts,
	    trendstore::get_trend_store_part,
	    trendstore::find_trend_store_part,
	    trendstore::get_trend_stores,
	    trendstore::get_trend_store,
	    trendstore::post_trend_store_part,
	    datasource::get_data_sources,
	    datasource::get_data_source,
	    entitytype::get_entity_types,
	    entitytype::get_entity_type,
	    kpi::get_kpis,
	    kpi::post_kpi,
	    kpi::update_kpi,
        ),
        components(TrendMaterializationSourceData, TrendMaterializationDef,
		   TrendViewMaterializationFull, TrendFunctionMaterializationFull,
		   TrendViewMaterializationData, TrendFunctionMaterializationData,
		   TrendFull, GeneratedTrendFull, TrendStorePartFull, TrendStoreFull,
		   DataSource, EntityType, KpiData),
        tags(
            (name = "Trend Materialization", description = "Trend materialization management endpoints.")
        ),
    )]
    struct ApiDoc;

    let manager = PostgresConnectionManager::new(
        "host=127.0.0.1 user=postgres dbname=minerva"
            .parse()
            .unwrap(),
        NoTls,
    );
    let pool = bb8::Pool::builder().build(manager).await.unwrap();

    let openapi = ApiDoc::openapi();

    HttpServer::new(move || {
        let cors = Cors::permissive()
            .allowed_methods(vec!["GET", "POST", "PUT", "DELETE"])
            .max_age(3600);
        App::new()
            .wrap(cors)
            .wrap(Logger::default())
            .app_data(web::Data::new(pool.clone()))
            .service(
                SwaggerUi::new("/swagger-ui/{_:.*}").url("/api-doc/openapi.json", openapi.clone()),
            )
            .service(get_trend_view_materializations)
            .service(get_trend_view_materialization)
            .service(get_trend_function_materializations)
            .service(get_trend_function_materialization)
            .service(get_trend_materializations)
            .service(post_trend_view_materialization)
            .service(post_trend_function_materialization)
            .service(delete_trend_view_materialization)
            .service(delete_trend_function_materialization)
            .service(update_trend_function_materialization)
            .service(update_trend_view_materialization)
            .service(get_trend_store_parts)
            .service(get_trend_store_part)
            .service(find_trend_store_part)
            .service(post_trend_store_part)
            .service(get_trend_stores)
            .service(get_trend_store)
            .service(get_data_sources)
            .service(get_data_source)
            .service(get_entity_types)
            .service(get_entity_type)
            .service(get_kpis)
            .service(post_kpi)
            .service(update_kpi)
    })
    .bind(("127.0.0.1", 8000))?
    .run()
    .await
}
