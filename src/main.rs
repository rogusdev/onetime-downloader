
use dotenv::dotenv;

use maplit::hashmap;

use std::{env, io};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use rand::Rng;
use bytes::{Bytes};

use serde::{Serialize, Deserialize};
use serde::ser::{Serializer, SerializeStruct};

use rusoto_core::{Region};
use rusoto_dynamodb::{
    DynamoDb,
    DynamoDbClient,
    AttributeValue,
    GetItemInput,
    PutItemInput,
    ScanInput
};

// https://actix.rs/
// very fast framework: https://www.techempower.com/benchmarks/#section=data-r19
use actix_web::{web, App, Error as ActixError, HttpRequest, HttpResponse, HttpServer};
use actix_multipart::{Field, Multipart};
use futures::{StreamExt, TryStreamExt}; // adds... something for multipart processsing

//use async_trait::async_trait;



// postgres mapping: https://github.com/Dowwie/tokio-postgres-mapper
// actix example using postgres: https://github.com/actix/examples/tree/master/async_pg

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

// #[derive(Debug, Clone)]
// struct FixedTimeProvider {
//     fixed_unix_ts_ms: u64,
// }

// impl FixedTimeProvider {//TimeProvider for
//     fn unix_ts_ms (&self) -> u64 {
//         self.fixed_unix_ts_ms
//     }
// }

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

// https://serde.rs/impl-serialize.html
impl Serialize for OnetimeFile {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("OnetimeFile", 4)?;
        state.serialize_field("filename", &self.filename)?;
        // only size of contents because we don't want to send entire files back... (and no default serializer for bytes)
        state.serialize_field("contents_len", &self.contents.len())?;
        state.serialize_field("created_at", &self.created_at)?;
        state.serialize_field("updated_at", &self.updated_at)?;
        state.end()
    }
}

#[derive(Debug, Clone, Serialize)]
struct OnetimeLink {
    token: String,
    filename: String,
    created_at: u64,
    downloaded_at: Option<u64>,
    ip_address: Option<String>,
}

#[derive(Deserialize)]
struct CreateLink {
    filename: String,
}

// #[async_trait]
// trait OnetimeStorage {
//     async fn add_file (&self, filename: String, contents: Bytes) -> Result<bool, String>;
//     async fn list_files (&self) -> Result<Vec<OnetimeFile>, String>;
//     async fn get_file (&self, filename: String) -> Result<OnetimeFile, String>;
//     async fn add_link (&self, link: String, filename: String) -> Result<bool, String>;
//     async fn list_links (&self) -> Result<Vec<OnetimeLink>, String>;
//     async fn get_link (&self, token: String) -> Result<OnetimeLink, String>;
// }

// ----------------------------------------------------------------

#[derive(Clone)]
struct DynamodbStorage {
    time_provider: SystemTimeProvider,
    files_table: String,
    links_table: String,
    client: DynamoDbClient,
}

fn ddb_val_s (val: String) -> AttributeValue {
    AttributeValue {
        s: Some(val),
        ..Default::default()
    }
}

fn ddb_val_n (val: u64) -> AttributeValue {
    AttributeValue {
        n: Some(val.to_string()),
        ..Default::default()
    }
}

fn ddb_val_b (val: Bytes) -> AttributeValue {
    AttributeValue {
        b: Some(val),
        ..Default::default()
    }
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

fn ddb_attr_os (attributes: &HashMap<String, AttributeValue>, field: &String) -> Result<Option<String>, String> {
    match attributes.get(field) {
        None => Ok(None),
        Some(val) => val.s.clone().ok_or(format!("Empty field {}", field)).map(|s| Some(s))
    }
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

fn ddb_attr_on (attributes: &HashMap<String, AttributeValue>, field: &String) -> Result<Option<u64>, String> {
    match attributes.get(field) {
        None => Ok(None),
        Some(val) => match val.n.clone() {
            None => Err(format!("Empty field {}", field)),
            Some(val) => match val.parse::<u64>() {
                Err(why) => Err(format!("Field {} is not a number {}", field, why)),
                Ok(val) => Ok(Some(val)),
            }
        }
    }
}

impl DynamodbStorage {
    const DEFAULT_TABLE_FILES: &'static str = "Onetime.Files";
    const DEFAULT_TABLE_LINKS: &'static str = "Onetime.Links";

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

        match self.client.put_item(request).await {
            Err(why) => Err(format!("Add file failed: {}", why.to_string())),
            Ok(_) => Ok(true)
        }
    }

    fn collect_files (attributes_vec: Vec<HashMap<String, AttributeValue>>) -> Result<Vec<OnetimeFile>, String>  {
        let mut vec = Vec::new();
        // https://stackoverflow.com/questions/34733811/what-is-the-difference-between-iter-and-into-iter
        for attributes in attributes_vec.into_iter() {
            match Self::build_file(attributes) {
                Err(why) => return Err(format!("Failed collecting files: {}", why)),
                Ok(file) => vec.push(file),
            }
        }
        Ok(vec)
    }

    async fn list_files (&self) -> Result<Vec<OnetimeFile>, String>  {
        let projection_expression = [
            Self::FIELD_FILENAME,
            Self::FIELD_CONTENTS,
            Self::FIELD_CREATED_AT,
            Self::FIELD_UPDATED_AT,
        ].join(", ");

        // https://docs.rs/rusoto_dynamodb/0.45.0/rusoto_dynamodb/
        let request = ScanInput {
            projection_expression: Some(projection_expression),
            table_name: self.files_table.clone(),
            ..Default::default()
        };

        match self.client.scan(request).await {
            Err(why) => Err(format!("List files failed: {}", why.to_string())),
            Ok(output) => match output.items {
                None => Err("No files found".to_string()),
                Some(attributes_vec) => Self::collect_files(attributes_vec),
            }
        }
    }

    fn build_file (
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
            Err(why) => Err(format!("Get file failed: {}", why.to_string())),
            Ok(output) => match output.item {
                None => Err("File not found".to_string()),
                Some(attributes) => Self::build_file(attributes),
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
        let mut item = hashmap! {
            Self::FIELD_TOKEN.to_string() => ddb_val_s(link.token),
            Self::FIELD_FILENAME.to_string() => ddb_val_s(link.filename),
            Self::FIELD_CREATED_AT.to_string() => ddb_val_n(link.created_at),
        };
        if let Some(downloaded_at) = link.downloaded_at {
            item.insert(Self::FIELD_DOWNLOADED_AT.to_string(), ddb_val_n(downloaded_at));
        }
        if let Some(ip_address) = link.ip_address {
            item.insert(Self::FIELD_IP_ADDRESS.to_string(), ddb_val_s(ip_address));
        }
        // print!("add link item {:?}", item);

        let request = PutItemInput {
            item: item,
            table_name: self.links_table.clone(),
            ..Default::default()
        };
        // print!("add link request {:?}", request);

        match self.client.put_item(request).await {
            Err(why) => Err(format!("Add link failed: {}", why.to_string())),
            Ok(_) => Ok(true)
        }
    }

    fn collect_links (attributes_vec: Vec<HashMap<String, AttributeValue>>) -> Result<Vec<OnetimeLink>, String>  {
        let mut vec = Vec::new();
        for attributes in attributes_vec.into_iter() {
            match Self::build_link(attributes) {
                Err(why) => return Err(format!("Failed collecting links: {}", why)),
                Ok(link) => vec.push(link),
            }
        }
        Ok(vec)
    }

    async fn list_links (&self) -> Result<Vec<OnetimeLink>, String> {
        const TOKEN_SUBSTITUTE: &'static str = "#Token";

        let expression_attribute_names = hashmap! {
            TOKEN_SUBSTITUTE.to_string() => Self::FIELD_TOKEN.to_string(),
        };

        let projection_expression = [
            TOKEN_SUBSTITUTE,
            Self::FIELD_FILENAME,
            Self::FIELD_CREATED_AT,
            Self::FIELD_DOWNLOADED_AT,
            Self::FIELD_IP_ADDRESS,
        ].join(", ");

        // https://docs.rs/rusoto_dynamodb/0.45.0/rusoto_dynamodb/
        let request = ScanInput {
            projection_expression: Some(projection_expression),
            expression_attribute_names: Some(expression_attribute_names),
            table_name: self.links_table.clone(),
            ..Default::default()
        };

        match self.client.scan(request).await {
            Err(why) => Err(format!("List links failed: {}", why.to_string())),
            Ok(output) => match output.items {
                None => Err("No links found".to_string()),
                Some(attributes_vec) => Self::collect_links(attributes_vec),
            }
        }
    }

    fn build_link (
        attributes: HashMap<String, AttributeValue>
    ) -> Result<OnetimeLink, String> {
        let token = ddb_attr_s(&attributes, &Self::FIELD_TOKEN.to_string())?;
        let filename = ddb_attr_s(&attributes, &Self::FIELD_FILENAME.to_string())?;
        let created_at = ddb_attr_n(&attributes, &Self::FIELD_CREATED_AT.to_string())?;
        let downloaded_at = ddb_attr_on(&attributes, &Self::FIELD_DOWNLOADED_AT.to_string())?;
        let ip_address = ddb_attr_os(&attributes, &Self::FIELD_IP_ADDRESS.to_string())?;

        Ok(OnetimeLink {
            token: token,
            filename: filename,
            created_at: created_at,
            downloaded_at: downloaded_at,
            ip_address: ip_address,
        })
    }

    async fn get_link (
        &self,
        token: String,
    ) -> Result<OnetimeLink, String> {
        // https://www.rusoto.org/futures.html has example uses
        // ... maybe use https://docs.rs/crate/serde_dynamodb/0.6.0 ?
        let request = GetItemInput {
            key: self.token_key(token),
            table_name: self.links_table.clone(),
            ..Default::default()
        };

        match self.client.get_item(request).await {
            Err(why) => Err(format!("Get link failed: {}", why.to_string())),
            Ok(output) => match output.item {
                None => Err("Link not found".to_string()),
                Some(attributes) => Self::build_link(attributes),
            }
        }
    }

    async fn mark_downloaded (
        &self,
        link: OnetimeLink,
        ip_address: String,
        downloaded_at: u64
    ) -> Result<bool, String> {
        let item = hashmap! {
            Self::FIELD_TOKEN.to_string() => ddb_val_s(link.token),
            Self::FIELD_FILENAME.to_string() => ddb_val_s(link.filename),
            Self::FIELD_CREATED_AT.to_string() => ddb_val_n(link.created_at),
            Self::FIELD_DOWNLOADED_AT.to_string() => ddb_val_n(downloaded_at),
            Self::FIELD_IP_ADDRESS.to_string() => ddb_val_s(ip_address),
        };

        let request = PutItemInput {
            item: item,
            table_name: self.links_table.clone(),
            return_values: Some("ALL_OLD".to_string()),
            ..Default::default()
        };

        match self.client.put_item(request).await {
            Err(why) => Err(format!("Mark downloaded put failed: {}", why.to_string())),
            Ok(output) => match output.attributes {
                None => Ok(false),
                Some(attributes) => match Self::build_link(attributes) {
                    Err(why) => Err(format!("Mark downloaded build failed: {}", why.to_string())),
                    Ok(link) => Ok(link.downloaded_at.is_some()),
                },
            }
        }
    }
}

// ----------------------------------------------------------------

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
    if valid_api_key {
        Ok(true)
    } else {
        Err(HttpResponse::Unauthorized().body("Invalid or missing api key!"))
    }
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
) -> Result<web::Json<Vec<OnetimeFile>>, HttpResponse> {
    println!("list files");
    check_api_key(&req, service.config.api_key_files.as_str())?;

    match service.storage.list_files().await {
        Ok(files) => Ok(web::Json(files)),
        Err(why) => Err(HttpResponse::InternalServerError().body(format!("Something went wrong! {}", why))),
    }
}

async fn list_links (
    req: HttpRequest,
    service: web::Data<OnetimeDownloaderService>,
) -> Result<web::Json<Vec<OnetimeLink>>, HttpResponse> {
    println!("list links");
    check_api_key(&req, service.config.api_key_links.as_str())?;

    match service.storage.list_links().await {
        Ok(links) => Ok(web::Json(links)),
        Err(why) => Err(HttpResponse::InternalServerError().body(format!("Something went wrong! {}", why))),
    }
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

        let file = OnetimeFile {
            filename: filename.unwrap(),
            contents: contents.unwrap(),
            created_at: now,
            updated_at: now,
        };

        match service.storage.add_file(file).await {
            Ok(_) => Ok(HttpResponse::Ok().body("added file")),
            Err(why) => Ok(HttpResponse::InternalServerError().body(format!("Something went wrong! {}", why))),
        }
    } else {
        Ok(HttpResponse::BadRequest().body("No filename or file contents provided!"))
    }
}

async fn add_link (
    req: HttpRequest,
    payload: web::Json<CreateLink>,
    service: web::Data<OnetimeDownloaderService>,
) -> Result<HttpResponse, HttpResponse> {
    println!("add link");
    check_api_key(&req, service.config.api_key_links.as_str())?;
    check_rate_limit(&req)?;

    // TODO validate filename is stored file
    if true {
        let now = service.time_provider.unix_ts_ms();
        // https://rust-lang-nursery.github.io/rust-cookbook/algorithms/randomness.html
        let n: u64 = rand::thread_rng().gen();

        let token = format!("{:016x}{:016x}", now, n);
        println!("token {}", token);
        let url = format!("/download/{}", token);

        let link = OnetimeLink {
            filename: payload.filename.clone(),
            token: token,
            created_at: now,
            downloaded_at: None,
            ip_address: None,
        };

        match service.storage.add_link(link).await {
            Ok(_) => Ok(
                HttpResponse::Ok()
                    .content_type("text/plain")
                    .body(url)
            ),
            Err(why) => Err(HttpResponse::InternalServerError().body(format!("Something went wrong! {}", why))),
        }
    } else {
        Err(HttpResponse::BadRequest().body("Invalid filename for link!"))
    }
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
    let ip_address = req.connection_info().remote().unwrap().to_string();
    println!("downloading... {} by {}", token, ip_address);

    let not_found_file = format!("Could not find file for link {}", token);
    let link = match service.storage.get_link(token).await {
        Ok(link) => link,
        Err(why) => return HttpResponse::NotFound().body(
            format!("{}: {}",  not_found_file, why)
        )
    };

    if link.downloaded_at.is_some() {
        return HttpResponse::NotFound().body("Already downloaded");
    }

    let now = service.time_provider.unix_ts_ms();
    let filename = link.filename.clone();
    match service.storage.mark_downloaded(link, ip_address, now).await {
        Err(why) => return HttpResponse::InternalServerError().body(format!("Something went wrong! {}", why)),
        Ok(already_downloaded) => if already_downloaded {
            return HttpResponse::NotFound().body("Already downloaded race");
        },
    }

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

// ----------------------------------------------------------------

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
                web::route().to(|| HttpResponse::NotFound().body("404 DNE"))
            )
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
