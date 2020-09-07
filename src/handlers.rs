
use rand::Rng;
use bytes::{Bytes};
// https://actix.rs/
// very fast framework: https://www.techempower.com/benchmarks/#section=data-r19
use actix_web::{web, HttpRequest, HttpResponse};
use actix_multipart::{Field, Multipart};
use futures::{StreamExt, TryStreamExt}; // adds... something for multipart processsing

use crate::models::{CreateLink, OnetimeDownloaderService, OnetimeFile, OnetimeLink};


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

pub async fn list_files (
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

pub async fn list_links (
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

pub async fn add_file (
    req: HttpRequest,
    mut payload: Multipart,
    service: web::Data<OnetimeDownloaderService>,
) -> Result<HttpResponse, HttpResponse> {
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

pub async fn add_link (
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

pub async fn download_link (
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

pub fn not_found () -> HttpResponse {
    HttpResponse::NotFound().body("404 DNE")
}
