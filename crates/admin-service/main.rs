use std::env;

use actix_cors::Cors;
use actix_web::{middleware::Logger, web, App, HttpServer};

use tokio_postgres::{Config, config::SslMode};

use bb8;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};

use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use minerva::error::{ConfigurationError, Error, RuntimeError};

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
    get_trend_stores, get_trends, get_trends_by_entity_type, post_trend_store_part,
    GeneratedTrendFull, TrendFull, TrendStoreFull, TrendStorePartFull,
};

mod datasource;
use datasource::{get_data_source, get_data_sources, DataSource};

mod entitytype;
use entitytype::{get_entity_type, get_entity_types, EntityType};

mod kpi;
use kpi::{delete_kpi, get_kpi, get_kpis, post_kpi, update_kpi, KpiImplementedData, KpiRawData};

mod error;

static ENV_DB_CONN: &str = "MINERVA_DB_CONN";


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    #[derive(OpenApi)]
    #[openapi(
        paths(
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
            trendstore::get_trends,
            trendstore::get_trends_by_entity_type,
            datasource::get_data_sources,
            datasource::get_data_source,
            entitytype::get_entity_types,
            entitytype::get_entity_type,
            kpi::get_kpis,
            kpi::post_kpi,
            kpi::update_kpi,
            kpi::get_kpi,
            kpi::delete_kpi,
        ),
        components(
            schemas(
                TrendMaterializationSourceData, TrendMaterializationDef,
                TrendViewMaterializationFull, TrendFunctionMaterializationFull,
                TrendViewMaterializationData, TrendFunctionMaterializationData,
                TrendFull, GeneratedTrendFull, TrendStorePartFull, TrendStoreFull,
                DataSource, EntityType, KpiRawData, KpiImplementedData
            )
        ),
        tags(
            (name = "Trend Materialization", description = "Trend materialization management endpoints.")
        ),
    )]
    struct ApiDoc;

    let pool = connect_db().await.unwrap();

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
            .service(get_trends)
            .service(get_trends_by_entity_type)
            .service(get_data_sources)
            .service(get_data_source)
            .service(get_entity_types)
            .service(get_entity_type)
            .service(get_kpis)
            .service(post_kpi)
            .service(update_kpi)
            .service(get_kpi)
            .service(delete_kpi)
    })
    .bind(("127.0.0.1", 8000))?
    .run()
    .await
}


fn get_db_config() -> Result<Config, Error> {
    let config = match env::var(ENV_DB_CONN) {
        Ok(value) => Config::new().options(&value).clone(),
        Err(_) => {
            // No single environment variable set, let's check for psql settings
            let port: u16 = env::var("PGPORT").unwrap_or("5432".into()).parse().unwrap();
            let mut config = Config::new();

            let env_sslmode = env::var("PGSSLMODE").unwrap_or("prefer".into());

            let sslmode = match env_sslmode.to_lowercase().as_str() {
                "disable" => SslMode::Disable,
                "prefer" => SslMode::Prefer,
                "require" => SslMode::Require,
                _ => return Err(Error::Configuration(ConfigurationError { msg: format!("Unsupported SSL mode '{}'", &env_sslmode) }))
            };

            let config = config
                .host(&env::var("PGHOST").unwrap_or("localhost".into()))
                .port(port)
                .user(&env::var("PGUSER").unwrap_or("postgres".into()))
                .dbname(&env::var("PGDATABASE").unwrap_or("postgres".into()))
                .ssl_mode(sslmode);

            let pg_password = env::var("PGPASSWORD");

            match pg_password {
                Ok(password) => config.password(password).clone(),
                Err(_) => config.clone(),
            }
        }
    };

    Ok(config)
}

async fn connect_db() -> Result<bb8::Pool<PostgresConnectionManager<NoTls>>, Error> {
    connect_to_db(&get_db_config()?).await
}

async fn connect_to_db(config: &Config) -> Result<bb8::Pool<PostgresConnectionManager<NoTls>>, Error> {
    let manager = PostgresConnectionManager::new(
        config.clone(),
        NoTls,
    );
    let pool = bb8::Pool::builder().build(manager).await.unwrap();

    Ok(pool)
}
