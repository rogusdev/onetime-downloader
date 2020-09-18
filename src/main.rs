
// https://stackoverflow.com/questions/56714619/including-a-file-from-another-that-is-not-main-rs-nor-lib-rs
mod time_provider;
mod models;
mod storage;
mod handlers;

use dotenv::dotenv;
use actix_web::{web, App, HttpServer};

use crate::time_provider::{SystemTimeProvider, TimeProvider};
use crate::models::{OnetimeDownloaderConfig, OnetimeDownloaderService, OnetimeStorage};
use crate::storage::{dynamodb, invalid, postgres};
use crate::handlers::{list_files, list_links, add_file, add_link, download_link, not_found, delete_file, delete_link};


fn build_service () -> OnetimeDownloaderService {
    // https://stackoverflow.com/questions/28219519/are-polymorphic-variables-allowed
    let time_provider: Box<dyn TimeProvider> = Box::new(SystemTimeProvider {});

    let config = OnetimeDownloaderConfig::from_env();
    println!("config {:?}", config);

    // https://stackoverflow.com/questions/25383488/how-to-match-a-string-against-string-literals-in-rust
    let storage: Box<dyn OnetimeStorage> = match config.provider.as_str() {
        "dynamodb" => Box::new(dynamodb::Storage::from_env(time_provider.clone())),
        "postgres" => match postgres::Storage::from_env(time_provider.clone()) {
            Err(why) => Box::new(invalid::Storage { error: format!("Invalid postgres storage provider! {}", why) }),
            Ok(storage) => Box::new(storage),
        },
        _ => Box::new(invalid::Storage { error: format!("Invalid or no storage provider given! '{}'", config.provider) })
    };

    println!("created storage: {}", storage.name());

    OnetimeDownloaderService {
        time_provider: time_provider,
        config: config,
        storage: storage,
    }
}

#[actix_rt::main]
async fn main () -> std::io::Result<()> {
    dotenv().ok();

    HttpServer::new(|| {
        App::new()
            .data(build_service())
            // https://actix.rs/docs/application/
            .service(
                web::scope("/api")
                    .route("files", web::get().to(list_files))
                    .route("links", web::get().to(list_links))
                    .route("files", web::post().to(add_file))
                    .route("links", web::post().to(add_link))
                    .route("files/{filename}", web::delete().to(delete_file))
                    .route("links/{token}", web::delete().to(delete_link))
            )
            .route("download/{token}", web::get().to(download_link))
            // https://github.com/actix/actix-website/blob/master/content/docs/url-dispatch.md
            .default_service(
                // https://docs.rs/actix-web/2.0.0/actix_web/struct.App.html#method.service
                web::route().to(not_found)
            )
    })
    // https://stackoverflow.com/questions/57177889/rust-actix-web-inside-docker-isnt-attainable-why/60361941#60361941
    // https://turreta.com/2020/07/03/deploy-actix-web-in-docker-container/
    .bind("0.0.0.0:8080")?
    .run()
    .await
}
