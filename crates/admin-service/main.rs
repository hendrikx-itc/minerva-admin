use actix_web::{get, web, App, HttpServer, Responder};

use bb8;
use bb8_postgres::{tokio_postgres::NoTls, PostgresConnectionManager};

#[get("/trendviewmaterializations/{name}")]
async fn greet(pool: web::Data<bb8::Pool<PostgresConnectionManager<NoTls>>>, name: web::Path<String>) -> impl Responder {
    let conn = pool.get().await.expect("could not get connection from pool");

    let rows = conn.query(
        "SELECT tsp.name FROM trend_directory.materialization m JOIN trend_directory.view_materialization vm ON vm.materialization_id = m.id JOIN trend_directory.trend_store_part tsp ON tsp.id = m.dst_trend_store_part_id",
        &[&name.to_string()]
    ).await.unwrap();

    let 
    for row in rows {
    
        let value: String = row.get(0);
        format!("Hello {}!", value)
    }

}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let manager = PostgresConnectionManager::new(
        "host=127.0.0.1 user=postgres".parse().unwrap(),
        NoTls,
    );
    let pool = bb8::Pool::builder().build(manager).await.unwrap();

    HttpServer::new(move || {
        App::new().app_data(web::Data::new(pool.clone()))
            .service(greet)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
