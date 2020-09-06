
// https://stackoverflow.com/questions/56714619/including-a-file-from-another-that-is-not-main-rs-nor-lib-rs
mod time_provider;
mod objects;
mod storage;
mod handlers;

use dotenv::dotenv;
use actix_web::{web, App, HttpServer};

use crate::time_provider::{SystemTimeProvider, TimeProvider};
use crate::objects::{OnetimeDownloaderConfig, OnetimeDownloaderService, OnetimeStorage};
use crate::storage::{dynamodb, invalid};//postgres
use crate::handlers::{list_files, list_links, add_file, add_link, download_link, not_found};


// consider mongo support: https://lib.rs/crates/mongodb

// http client: https://crates.io/crates/reqwest

// toki vs async-std ...vs smol? https://github.com/stjepang/smol
// https://www.reddit.com/r/rust/comments/dngig6/tokio_vs_asyncstd/

// maybe use warp instead of actix-web? https://github.com/seanmonstar/warp
// or gotham which leverages hyper, same as wrap: https://gotham.rs/

// rdkafka for publishing that way: https://lib.rs/crates/rdkafka

// json: https://github.com/serde-rs/json -- note json syntax for creating objects


/*

aws dynamodb delete-table \
    --profile rogusdev-chris \
    --table-name Onetime.Links


aws dynamodb create-table \
    --profile rogusdev-chris \
    --table-name Onetime.Files \
    --attribute-definitions \
        AttributeName=Filename,AttributeType=S \
    --key-schema \
        AttributeName=Filename,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

#        AttributeName=Contents,AttributeType=B \
#        AttributeName=CreatedAt,AttributeType=N \
#        AttributeName=UpdatedAt,AttributeType=N \

aws dynamodb create-table \
    --profile rogusdev-chris \
    --table-name Onetime.Links \
    --attribute-definitions \
        AttributeName=Token,AttributeType=S \
    --key-schema \
        AttributeName=Token,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

#        AttributeName=Filename,AttributeType=S \
#        AttributeName=CreatedAt,AttributeType=N \
#        AttributeName=DownloadedAt,AttributeType=N \
#        AttributeName=Ip,AttributeType=N \

*/


// https://github.com/actix/examples/blob/master/multipart/src/main.rs
// https://docs.rs/actori-multipart/0.2.0/actori_multipart/struct.Field.html
// https://docs.rs/actori-http/1.0.1/actori_http/http/header/struct.ContentDisposition.html
// match content_disposition.get_filename() {
//     Some(filename) => {
//         println!("filename {:?}", filename);
//         let filepath = format!("/tmp/{}", sanitize_filename::sanitize(&filename));
//         let mut f = web::block(|| fs::File::create(filepath)).await.unwrap();
//         // Field in turn is stream of *Bytes* object
//         // https://docs.rs/futures-core/0.3.1/futures_core/stream/trait.Stream.html
//         while let Some(chunk) = field.next().await {
//             let data = chunk.unwrap();
//             size += data.len();
//             if (size > MAX_FILE_SIZE) {
//                 println!("file too big!");
//             }
//             // filesystem operations are blocking, we have to use threadpool
//             f = web::block(move || f.write_all(&data).map(|_| f)).await?;
//         }
//     }
//     None => {
//         println!("not a file!");
//         let mut val = Vec::new();
//         while let Some(chunk) = field.next().await {
//             let data = chunk.unwrap();
//             size += data.len();
//             if (size > MAX_VALUE_SIZE) {
//                 println!("field value too big!");
//             }
//             val.append(&mut data.to_vec());
//         }
//         string_values.insert(field_name, String::from_utf8(val).unwrap());
//     }
// }


fn build_service () -> OnetimeDownloaderService {
    // https://stackoverflow.com/questions/28219519/are-polymorphic-variables-allowed
    let time_provider: Box<dyn TimeProvider> = Box::new(SystemTimeProvider {});

    let config = OnetimeDownloaderConfig::from_env();
    println!("config {:?}", config);

    // https://stackoverflow.com/questions/25383488/how-to-match-a-string-against-string-literals-in-rust
    let storage: Box<dyn OnetimeStorage> = match config.provider.as_str() {
        "dynamodb" => Box::new(dynamodb::Storage::from_env(time_provider.clone())),
//        "postgres" => Box::new(postgres::Storage::from_env(time_provider.clone())?),
        _ => Box::new(invalid::Storage { error: format!("Invalid or no storage provider given! '{}'", config.provider) })
    };

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
            )
            .route("download/{token}", web::get().to(download_link))
            // https://github.com/actix/actix-website/blob/master/content/docs/url-dispatch.md
            .default_service(
                // https://docs.rs/actix-web/2.0.0/actix_web/struct.App.html#method.service
                web::route().to(not_found)
            )
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
