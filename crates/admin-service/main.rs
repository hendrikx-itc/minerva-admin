use actix_web::{web, App, HttpServer, middleware::Logger,};

use bb8;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};

use utoipa::{
    OpenApi,
};
use utoipa_swagger_ui::SwaggerUi;


mod trendviewmaterialization;

use trendviewmaterialization::{TrendMaterializationSource, TrendViewMaterialization, get_trend_view_materializations};

mod trendstore;

use trendstore::{Trend, GeneratedTrend, TrendStorePart, get_trend_store_parts};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    #[derive(OpenApi)]
    #[openapi(
        handlers(
            trendviewmaterialization::get_trend_view_materializations,
	    trendstore::get_trend_store_parts,
        ),
        components(TrendMaterializationSource, TrendViewMaterialization, Trend, GeneratedTrend, TrendStorePart),
        tags(
            (name = "Trend Materialization", description = "Trend materialization management endpoints.")
        ),
    )]
    struct ApiDoc;

    let manager = PostgresConnectionManager::new(
        "host=127.0.0.1 user=postgres dbname=minerva".parse().unwrap(),
        NoTls,
    );
    let pool = bb8::Pool::builder().build(manager).await.unwrap();

    let openapi = ApiDoc::openapi();

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(web::Data::new(pool.clone()))
            .service(SwaggerUi::new("/swagger-ui/{_:.*}").url("/api-doc/openapi.json", openapi.clone()))
            .service(get_trend_view_materializations)
	    .service(get_trend_store_parts)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
