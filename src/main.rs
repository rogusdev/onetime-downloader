
use dotenv::dotenv;

use maplit::hashmap;

use std::{env, io};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use rand::Rng;
use bytes::{Bytes};

use rusoto_core::{Region};
use rusoto_dynamodb::{DynamoDb, DynamoDbClient, AttributeValue, GetItemInput, PutItemInput, ListTablesInput};

// https://actix.rs/
// very fast framework: https://www.techempower.com/benchmarks/#section=data-r19
use actix_web::{web, App, Error as ActixError, HttpRequest, HttpResponse, HttpServer};
use actix_multipart::{Field, Multipart};
use futures::{StreamExt, TryStreamExt}; // adds... something for multipart processsing

//use async_trait::async_trait;


/*

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

trait TimeProvider {
    fn unix_ts_ms (&self);
}

#[derive(Debug, Clone)]
struct SystemTimeProvider {
}

impl SystemTimeProvider {//TimeProvider for
    fn unix_ts_ms (&self) -> u64 {
        let dur = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        (dur.as_secs() * 1_000) + dur.subsec_millis() as u64
    }
}

#[derive(Debug, Clone)]
struct FixedTimeProvider {
    fixed_unix_ts_ms: u64,
}

impl FixedTimeProvider {//TimeProvider for
    fn unix_ts_ms (&self) -> u64 {
        self.fixed_unix_ts_ms
    }
}

#[derive(Debug, Clone)]
struct OnetimeDownloaderConfig {
    api_key_files: String,
    api_key_links: String,
    max_len_file: usize,
    max_len_value: usize,
}

impl OnetimeDownloaderConfig {
    const EMPTY_STRING: String = String::new();
    const DEFAULT_MAX_LEN_FILE: usize = 100000;
    const DEFAULT_MAX_LEN_VALUE: usize = 80;

    fn env_var_string (name: &str, default: String) -> String {
        env::var(name).unwrap_or(default)
    }

    fn env_var_parse<T : std::str::FromStr> (name: &str, default: T) -> T {
        match env::var(name) {
            Ok(s) => s.parse::<T>().unwrap_or(default),
            _ => default
        }
    }

    fn from_env () -> OnetimeDownloaderConfig {
        OnetimeDownloaderConfig {
            api_key_files: Self::env_var_string("FILES_API_KEY", Self::EMPTY_STRING),
            api_key_links: Self::env_var_string("LINKS_API_KEY", Self::EMPTY_STRING),
            max_len_file: Self::env_var_parse("FILE_MAX_LEN", Self::DEFAULT_MAX_LEN_FILE),
            max_len_value: Self::env_var_parse("VALUE_MAX_LEN", Self::DEFAULT_MAX_LEN_VALUE),
        }
    }
}

#[derive(Debug, Clone)]
struct OnetimeFile {
    filename: String,
    contents: Bytes,
    created_at: u64,
    updated_at: u64,
}

#[derive(Debug, Clone)]
struct OnetimeLink {
    token: String,
    filename: String,
    created_at: u64,
    downloaded_at: Option<u64>,
    ip_address: Option<String>,
}

#[derive(Clone)]
struct DynamodbStorage {
    time_provider: SystemTimeProvider,
    files_table: String,
    links_table: String,
    client: DynamoDbClient,
}

fn ddb_val_os (val: Option<String>) -> AttributeValue {
    AttributeValue {
        s: val,
        ..Default::default()
    }
}

fn ddb_val_s (val: String) -> AttributeValue {
    ddb_val_os(Some(val))
}

fn ddb_val_on (val: Option<String>) -> AttributeValue {
    AttributeValue {
        n: val,
        ..Default::default()
    }
}

fn ddb_val_n (val: u64) -> AttributeValue {
    ddb_val_on(Some(val.to_string()))
}

fn ddb_val_ob (val: Option<Bytes>) -> AttributeValue {
    AttributeValue {
        b: val,
        ..Default::default()
    }
}

fn ddb_val_b (val: Bytes) -> AttributeValue {
    ddb_val_ob(Some(val))
}

fn ddb_key_s (key: String, val: String) -> HashMap<String, AttributeValue> {
    hashmap! {
        key => ddb_val_s(val)
    }

    // let mut item = HashMap::new();
    // item.insert(key, ddb_val_s(val));
}

fn ddb_attr_s (attributes: &HashMap<String, AttributeValue>, field: &String) -> Result<String, String> {
    // clone because get returns Option<&V> (not Option<V>)
    //  and thus without clone, this attempts a move out of that (that fails to compile)
    //  https://doc.rust-lang.org/beta/std/collections/struct.HashMap.html#method.get
    attributes.get(field).ok_or(format!("Missing field {}", field))?.clone()
        .s.ok_or(format!("Empty field {}", field))
}

fn ddb_attr_b (attributes: &HashMap<String, AttributeValue>, field: &String) -> Result<Bytes, String> {
    attributes.get(field).ok_or(format!("Missing field {}", field))?.clone()
        .b.ok_or(format!("Empty field {}", field))
        .map(|s| Bytes::from(s))
}

fn ddb_attr_n (attributes: &HashMap<String, AttributeValue>, field: &String) -> Result<u64, String> {
    attributes.get(field).ok_or(format!("Missing field {}", field))?.clone()
        .n.ok_or(format!("Empty field {}", field))?
        .parse::<u64>().map_err(|why| format!("Field {} is not a number {}", field, why))
}

impl DynamodbStorage {
    const DEFAULT_TABLE_FILES: &'static str = "Ontetime.Files";
    const DEFAULT_TABLE_LINKS: &'static str = "Ontetime.Links";

    fn from_env (time_provider: SystemTimeProvider) -> DynamodbStorage {
        DynamodbStorage {
            time_provider: time_provider,
            files_table: OnetimeDownloaderConfig::env_var_string("DDB_FILES_TABLE", String::from(Self::DEFAULT_TABLE_FILES)),
            links_table: OnetimeDownloaderConfig::env_var_string("DDB_LINKS_TABLE", String::from(Self::DEFAULT_TABLE_LINKS)),
            // https://docs.rs/rusoto_dynamodb/0.44.0/rusoto_dynamodb/
            client: DynamoDbClient::new(Region::UsEast1),
        }
    }
}

// #[async_trait]
// trait OnetimeStorage {
//     async fn add_file (&self, filename: String, contents: Bytes) -> Result<bool, String>;
//     async fn list_files (&self) -> Result<Vec<OnetimeFile>, String>;
//     async fn get_file (&self, filename: String) -> Result<OnetimeFile, String>;
//     async fn add_link (&self, link: String, filename: String) -> Result<bool, String>;
//     async fn list_links (&self) -> Result<Vec<OnetimeLink>, String>;
//     async fn get_link (&self, token: String, ip: String) -> Result<OnetimeLink, String>;
// }

impl DynamodbStorage {//for OnetimeStorage
    const FIELD_FILENAME: &'static str = "Filename";
    const FIELD_CONTENTS: &'static str = "Contents";
    const FIELD_CREATED_AT: &'static str = "CreatedAt";
    const FIELD_UPDATED_AT: &'static str = "UpdatedAt";

    fn filename_key (&self, filename: String) -> HashMap<String, AttributeValue> {
        ddb_key_s(Self::FIELD_FILENAME.to_string(), filename)
    }

    async fn add_file (&self, file: OnetimeFile) -> Result<bool, String> {
        let item = hashmap! {
            Self::FIELD_FILENAME.to_string() => ddb_val_s(file.filename),
            Self::FIELD_CONTENTS.to_string() => ddb_val_b(file.contents),
            Self::FIELD_CREATED_AT.to_string() => ddb_val_n(file.created_at),
            Self::FIELD_UPDATED_AT.to_string() => ddb_val_n(file.updated_at),
        };

        let request = PutItemInput {
            item: item,
            table_name: self.files_table.clone(),
            ..Default::default()
        };

        match self.client.put_item(request).await {//.map(|output| true)
            Err(why) => Err(why.to_string()),
            Ok(output) => Ok(true)
        }
    }

    async fn list_files (&self) -> Result<Vec<OnetimeFile>, String>  {
        // https://docs.rs/rusoto_dynamodb/0.44.0/rusoto_dynamodb/
        let request = ListTablesInput::default();
        // https://rusoto.github.io/rusoto/rusoto_dynamodb/struct.ListTablesOutput.html
        let response = self.client.list_tables(request).await;
        println!("Tables (files) found: {:?}", response);

        let mut vec = Vec::new();
        vec.push(OnetimeFile {
            filename: String::from("file.zip"),
            contents: Bytes::from(&b"Hello world"[..]),
            created_at: 123,
            updated_at: 456,
        });

        Ok(vec)
    }

    fn build_file (
        &self,
        attributes: HashMap<String, AttributeValue>,
    ) -> Result<OnetimeFile, String> {
        let filename = ddb_attr_s(&attributes, &Self::FIELD_FILENAME.to_string())?;
        let contents = ddb_attr_b(&attributes, &Self::FIELD_CONTENTS.to_string())?;
        let created_at = ddb_attr_n(&attributes, &Self::FIELD_CREATED_AT.to_string())?;
        let updated_at = ddb_attr_n(&attributes, &Self::FIELD_UPDATED_AT.to_string())?;

        Ok(OnetimeFile {
            filename: filename,
            contents: contents,
            created_at: created_at,
            updated_at: updated_at,
        })
    }

    async fn get_file (&self, filename: String) -> Result<OnetimeFile, String>  {
        // https://www.rusoto.org/futures.html has example uses
        // ... maybe use https://docs.rs/crate/serde_dynamodb/0.6.0 ?
        let request = GetItemInput {
            key: self.filename_key(filename),
            table_name: self.files_table.clone(),
            ..Default::default()
        };

        match self.client.get_item(request).await {
            Err(why) => Err(why.to_string()),
            Ok(output) => match output.item {
                None => Err("File not found".to_string()),
                Some(attributes) => self.build_file(attributes),
            }
        }
    }

    const FIELD_TOKEN: &'static str = "Token";
    const FIELD_DOWNLOADED_AT: &'static str = "DownloadedAt";
    const FIELD_IP_ADDRESS: &'static str = "IpAddress";

    fn token_key (&self, token: String) -> HashMap<String, AttributeValue> {
        ddb_key_s(Self::FIELD_TOKEN.to_string(), token)
    }

    async fn add_link (&self, link: OnetimeLink) -> Result<bool, String> {
        let item = hashmap! {
            Self::FIELD_TOKEN.to_string() => ddb_val_s(link.token),
            Self::FIELD_FILENAME.to_string() => ddb_val_s(link.filename),
            Self::FIELD_CREATED_AT.to_string() => ddb_val_n(link.created_at),
            Self::FIELD_DOWNLOADED_AT.to_string() => ddb_val_on(link.downloaded_at.map(|v| v.to_string())),
            Self::FIELD_IP_ADDRESS.to_string() => ddb_val_os(link.ip_address),
        };

        let request = PutItemInput {
            item: item,
            table_name: self.links_table.clone(),
            ..Default::default()
        };

        match self.client.put_item(request).await {//.map(|output| true)
            Err(why) => Err(why.to_string()),
            Ok(output) => Ok(true)
        }
    }

    async fn list_links (&self) -> Result<Vec<OnetimeLink>, String> {
        let request = ListTablesInput::default();
        // https://rusoto.github.io/rusoto/rusoto_dynamodb/struct.ListTablesOutput.html
        let response = self.client.list_tables(request).await;
        println!("Tables (links) found: {:?}", response);

        let mut vec = Vec::new();
        vec.push(OnetimeLink {
            filename: String::from("file.zip"),
            token: String::from("abc123"),
            created_at: 123,
            downloaded_at: None,
            ip_address: None,
        });

        Ok(vec)
    }

    fn build_link (
        &self,
        attributes: HashMap<String, AttributeValue>,
        ip_address: String,
    ) -> Result<OnetimeLink, String> {
        let token = ddb_attr_s(&attributes, &Self::FIELD_TOKEN.to_string())?;
        let filename = ddb_attr_s(&attributes, &Self::FIELD_FILENAME.to_string())?;
        let created_at = ddb_attr_n(&attributes, &Self::FIELD_CREATED_AT.to_string())?;
        // let downloaded_at = ddb_attr_on(attributes, &Self::FIELD_UPDATED_AT.to_string())?;
        // let ip_address = ddb_attr_os(attributes, &Self::FIELD_FILENAME.to_string())?;

        let now = self.time_provider.unix_ts_ms();

        Ok(OnetimeLink {
            token: token,
            filename: filename,
            created_at: created_at,
            downloaded_at: Some(now),
            ip_address: Some(ip_address),
        })
    }

    async fn get_link (
        &self,
        token: String,
        ip_address: String,
    ) -> Result<OnetimeLink, String> {
        // https://www.rusoto.org/futures.html has example uses
        // ... maybe use https://docs.rs/crate/serde_dynamodb/0.6.0 ?
        let request = GetItemInput {
            key: self.token_key(token),
            table_name: self.links_table.clone(),
            ..Default::default()
        };

        match self.client.get_item(request).await {
            Err(why) => Err(why.to_string()),
            Ok(output) => match output.item {
                None => Err("Link not found".to_string()),
                Some(attributes) => self.build_link(attributes, ip_address),
            }
        }
    }
}

#[derive(Clone)]
struct OnetimeDownloaderService {
    time_provider: SystemTimeProvider,
    config: OnetimeDownloaderConfig,
    storage: DynamodbStorage,
}


const API_KEY_HEADER: &'static str = "X-Api-Key";

fn check_api_key (req: &HttpRequest, api_key: &str) -> Result<bool, HttpResponse> {
    let valid_api_key = match req.headers().get(API_KEY_HEADER) {
        Some(v) => v == api_key,
        _ => false
    };
    if !valid_api_key {
        return Err(HttpResponse::Unauthorized().body("Invalid or missing api key!"))
    }
    Ok(true)
}

fn check_rate_limit (req: &HttpRequest) -> Result<bool, HttpResponse> {
    let valid_ip = match req.connection_info().remote() {
        Some(ip) => ip != "0.0.0.0",
        _ => false
    };
    if valid_ip {
        Ok(true)
    } else {
        Err(HttpResponse::TooManyRequests().finish())
    }
}

async fn list_files (
    req: HttpRequest,
    service: web::Data<OnetimeDownloaderService>,
) -> Result<HttpResponse, ActixError> {
    println!("list files");

    service.storage.list_files().await;

    Ok(HttpResponse::Ok().body("list files!"))
}

async fn list_links (
    req: HttpRequest,
    service: web::Data<OnetimeDownloaderService>,
) -> Result<HttpResponse, ActixError> {
    println!("list links");

    service.storage.list_links().await;

    Ok(HttpResponse::Ok().body("list links!"))
}

async fn collect_chunks (mut field: Field, max: usize) -> Result<Vec<u8>, HttpResponse> {
    let mut size = 0;
    let mut val = Vec::new();
    while let Some(chunk) = field.next().await {
        let data = chunk.unwrap();
        size += data.len();
        if size > max {
            return Err(HttpResponse::BadRequest().body(format!("field value too big! {}", size)))
        }
        val.append(&mut data.to_vec());
    }
    Ok(val)
}

async fn add_file (
    req: HttpRequest,
    mut payload: Multipart,
    service: web::Data<OnetimeDownloaderService>,
) -> Result<HttpResponse, ActixError> {
    println!("add file");
    check_api_key(&req, service.config.api_key_files.as_str())?;
    check_rate_limit(&req)?;

    let mut filename: Option<String> = None;
    let mut contents: Option<Bytes> = None;

    while let Ok(Some(field)) = payload.try_next().await {
        let content_disposition = field.content_disposition().unwrap();
        let field_name = content_disposition.get_name().unwrap().to_owned();

        match content_disposition.get_filename() {
            Some(filename) => {
                println!("'{}' filename '{}'", field_name, filename);
                if field_name == "file" {
                    let val = collect_chunks(field, service.config.max_len_file).await?;
                    println!("file:\n{:?}", val);
                    contents = Some(Bytes::from(val));
                }
            }
            None => {
                println!("'{}' not a file!", field_name);
                if field_name == "filename" {
                    let val = collect_chunks(field, service.config.max_len_value).await?;
                    filename = Some(String::from_utf8(val).unwrap());
                }
            }
        }
    }

    if filename.is_some() && contents.is_some() {
        let now = service.time_provider.unix_ts_ms();

        let onetime_file = OnetimeFile {
            filename: filename.unwrap(),
            contents: contents.unwrap(),
            created_at: now,
            updated_at: now,
        };

        service.storage.add_file(onetime_file).await;
    }

    Ok(HttpResponse::Ok().body("added file"))
}

async fn add_link (
    req: HttpRequest,
    mut payload: Multipart,
    service: web::Data<OnetimeDownloaderService>,
) -> Result<HttpResponse, HttpResponse> {
    println!("add link");
    check_api_key(&req, service.config.api_key_links.as_str())?;
    check_rate_limit(&req)?;

    let mut filename: Option<String> = None;

    while let Ok(Some(field)) = payload.try_next().await {
        let content_disposition = field.content_disposition().unwrap();
        let field_name = content_disposition.get_name().unwrap().to_owned();

        match content_disposition.get_filename() {
            Some(filename) => {
                println!("'{}' filename '{}'", field_name, filename);
            }
            None => {
                println!("'{}' not a file!", field_name);
                if field_name == "filename" {
                    let val = collect_chunks(field, service.config.max_len_value).await?;
                    filename = Some(String::from_utf8(val).unwrap());
                }
            }
        }
    }

    if filename.is_some() {
        // TODO validate filename is stored file
        if true {
            let now = service.time_provider.unix_ts_ms();
            // https://rust-lang-nursery.github.io/rust-cookbook/algorithms/randomness.html
            let n: u64 = rand::thread_rng().gen();

            let token = format!("{:016x}{:016x}", now, n);
            println!("token {}", token);
            let url = format!("/download/{}", token);

            let link = OnetimeLink {
                filename: filename.unwrap(),
                token: token,
                created_at: now,
                downloaded_at: None,
                ip_address: None,
            };

            service.storage.add_link(link).await;

            // https://actix.rs/docs/response/
            return Ok(
                HttpResponse::Ok()
                    .content_type("text/plain")
                    .body(url)
            );
        }
    }

    Err(HttpResponse::BadRequest().body("Must provide filename for file"))
}

async fn download_link (
    req: HttpRequest,
    service: web::Data<OnetimeDownloaderService>,
) -> HttpResponse {
    println!("download link");
    if let Err(badreq) = check_rate_limit(&req) {
        return badreq
    }

    let token = req.match_info().get("token").unwrap().to_string();
    let ip = req.connection_info().remote().unwrap().to_string();
    println!("downloading... {} by {}", token, ip);

    let not_found_file = format!("Could not find file for link {}", token);
    let filename = match service.storage.get_link(token, ip).await {
        Ok(link) => link.filename,
        Err(why) => return HttpResponse::NotFound().body(
            format!("{}: {}",  not_found_file, why)
        )
    };

    let not_found_contents = format!("Could not find contents for filename {}", filename);
    let contents = match service.storage.get_file(filename).await {
        Ok(file) => file.contents,
        Err(why) => return HttpResponse::NotFound().body(
            format!("{}: {}", not_found_contents, why)
        )
    };

    // https://github.com/actix/examples/blob/master/basics/src/main.rs
    HttpResponse::Ok()
        .content_type("application/octet-stream")
        .body(contents)
}

fn build_service () -> OnetimeDownloaderService {
    let time_provider = SystemTimeProvider {};

    let config = OnetimeDownloaderConfig::from_env();
    println!("config {:?}", config);

    // TODO: what I want is to have a single instance get passed as traits (interfaces) that can be swapped out by config
    let storage = DynamodbStorage::from_env(SystemTimeProvider {});

    OnetimeDownloaderService {
        time_provider: time_provider,
        config: config,
        storage: storage,
    }
}


#[actix_rt::main]
async fn main () -> io::Result<()> {
    dotenv().ok();

    HttpServer::new(|| {
        App::new()
            .data(build_service())
            .service(
                web::scope("/api")
                    .route("files", web::get().to(list_files))
                    .route("links", web::get().to(list_links))
                    .route("files", web::post().to(add_file))
                    .route("links", web::post().to(add_link))
                    .route("download/{token}", web::get().to(download_link)),
            )
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
