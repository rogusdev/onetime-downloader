
use std::convert::TryFrom;
use bytes::{Bytes};
use async_trait::async_trait;

use deadpool_postgres::{Client, Config, Pool};
use tokio_postgres::{NoTls, row::Row};

use crate::time_provider::TimeProvider;
use crate::models::{MyError, OnetimeDownloaderConfig, OnetimeFile, OnetimeLink, OnetimeStorage};
use super::util::{try_from_vec};


const DEFAULT_SCHEMA: &'static str = "onetime";
const DEFAULT_TABLE_FILES: &'static str = "files";
const DEFAULT_TABLE_LINKS: &'static str = "links";

const DEFAULT_HOST: &'static str = "postgres";
const DEFAULT_PORT: &'static str = "5432";
const DEFAULT_USER: &'static str = "postgres";
const DEFAULT_PASSWORD: &'static str = "";
const DEFAULT_DBNAME: &'static str = "postgres";

const FIELD_FILENAME: &'static str = "filename";
const FIELD_CONTENTS: &'static str = "contents";
const FIELD_CREATED_AT: &'static str = "created_at";
const FIELD_UPDATED_AT: &'static str = "updated_at";

const FIELD_TOKEN: &'static str = "token";
const FIELD_NOTE: &'static str = "note";
const FIELD_EXPIRES_AT: &'static str = "expires_at";
const FIELD_DOWNLOADED_AT: &'static str = "downloaded_at";
const FIELD_IP_ADDRESS: &'static str = "ip_address";


#[derive(Clone)]
pub struct Storage {
    time_provider: Box<dyn TimeProvider>,
    schema: String,
    files_table: String,
    links_table: String,
    pool: Pool,
}

impl TryFrom<Row> for OnetimeFile {
    type Error = MyError;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        // consider: https://docs.rs/tokio-pg-mapper/0.1.8/tokio_pg_mapper/
        // https://docs.rs/tokio-postgres/0.5.5/tokio_postgres/row/struct.Row.html#method.try_get
        let filename = row.try_get(&FIELD_FILENAME).map_err(|why| format!("Could not get filename! {}", why))?;
        // https://docs.rs/tokio-postgres/0.5.0-alpha.1/tokio_postgres/types/trait.FromSql.html
        let contents: Vec<u8> = row.try_get(&FIELD_CONTENTS).map_err(|why| format!("Could not get contents! {}", why))?;
        let created_at = row.try_get(&FIELD_CREATED_AT).map_err(|why| format!("Could not get created_at! {}", why))?;
        let updated_at = row.try_get(&FIELD_UPDATED_AT).map_err(|why| format!("Could not get updated_at! {}", why))?;

        Ok(Self {
            filename: filename,
            contents: Bytes::from(contents),
            created_at: created_at,
            updated_at: updated_at,
        })
    }
}

impl TryFrom<Row> for OnetimeLink {
    type Error = MyError;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let token = row.try_get(&FIELD_TOKEN).map_err(|why| format!("Could not get {}! {}", FIELD_TOKEN, why))?;
        let filename = row.try_get(&FIELD_FILENAME).map_err(|why| format!("Could not get {}! {}", FIELD_FILENAME, why))?;
        let note = row.try_get(&FIELD_NOTE).map_err(|why| format!("Could not get {}! {}", FIELD_NOTE, why))?;
        let created_at = row.try_get(&FIELD_CREATED_AT).map_err(|why| format!("Could not get {}! {}", FIELD_CREATED_AT, why))?;
        let expires_at = row.try_get(&FIELD_EXPIRES_AT).map_err(|why| format!("Could not get {}! {}", FIELD_EXPIRES_AT, why))?;
        let downloaded_at = row.try_get(&FIELD_DOWNLOADED_AT).map_err(|why| format!("Could not get {}! {}", FIELD_DOWNLOADED_AT, why))?;
        let ip_address = row.try_get(&FIELD_IP_ADDRESS).map_err(|why| format!("Could not get {}! {}", FIELD_IP_ADDRESS, why))?;

        Ok(Self {
            token: token,
            filename: filename,
            note: note,
            created_at: created_at,
            expires_at: expires_at,
            downloaded_at: downloaded_at,
            ip_address: ip_address,
        })
    }
}

impl Storage {
    pub fn from_env (time_provider: Box<dyn TimeProvider>) -> Result<Self, MyError> {
        // https://crates.io/crates/deadpool-postgres
        let cfg = Config {
            host: Some(OnetimeDownloaderConfig::env_var_string("PG_HOST", String::from(DEFAULT_HOST))),
            port: Some(
                OnetimeDownloaderConfig::env_var_string("PG_PORT", String::from(DEFAULT_PORT))
                    .parse::<u16>().map_err(|why| format!("Port is not a valid number! {}", why))?
            ),
            user: Some(OnetimeDownloaderConfig::env_var_string("PG_USER", String::from(DEFAULT_USER))),
            password: Some(OnetimeDownloaderConfig::env_var_string("PG_PASS", String::from(DEFAULT_PASSWORD))),
            dbname: Some(OnetimeDownloaderConfig::env_var_string("PG_DBNAME", String::from(DEFAULT_DBNAME))),
            ..Default::default()
        };

        let storage = Self {
            time_provider: time_provider,
            schema: OnetimeDownloaderConfig::env_var_string("PG_SCHEMA", String::from(DEFAULT_SCHEMA)),
            files_table: OnetimeDownloaderConfig::env_var_string("PG_FILES_TABLE", String::from(DEFAULT_TABLE_FILES)),
            links_table: OnetimeDownloaderConfig::env_var_string("PG_LINKS_TABLE", String::from(DEFAULT_TABLE_LINKS)),
            pool: cfg.create_pool(NoTls).map_err(|why| format!("Failed creating pool: {}", why))?,
        };

        Ok(storage)
    }

    async fn client (&self) -> Result<Client, MyError> {
        self.pool.get().await.map_err(|why| format!("Failed creating client: {}", why))
    }
}

// https://github.com/dtolnay/async-trait#non-threadsafe-futures
#[async_trait(?Send)]
impl OnetimeStorage for Storage {
    fn name(&self) -> &'static str {
        "Postgres"
    }

    async fn add_file (&self, file: OnetimeFile) -> Result<bool, MyError> {
        match self.client().await?.execute(
            format!(
                "INSERT INTO {}.{} ({}, {}, {}, {}) VALUES ($1, $2, $3, $4)
                    ON CONFLICT ({}) DO UPDATE SET {}=$4, {}=$2",
                self.schema,
                self.files_table,
                FIELD_FILENAME,
                FIELD_CONTENTS,
                FIELD_CREATED_AT,
                FIELD_UPDATED_AT,

                FIELD_FILENAME,
                FIELD_UPDATED_AT,
                FIELD_CONTENTS,
            ).as_str(),
            &[
                &file.filename,
                &file.contents.as_ref(),
                &file.created_at,
                &file.updated_at,
            ],
        ).await {
            Err(why) => Err(format!("Add file failed: {}", why.to_string())),
            Ok(_) => Ok(true)
        }
    }

    async fn list_files (&self) -> Result<Vec<OnetimeFile>, MyError>  {
        match self.client().await?.query(
            format!(
                "SELECT {}, {}, {}, {} FROM {}.{}",
                FIELD_FILENAME,
                FIELD_CONTENTS,
                FIELD_CREATED_AT,
                FIELD_UPDATED_AT,
                self.schema,
                self.files_table,
            ).as_str(),
            &[
            ],
        ).await {
            Err(why) => Err(format!("List files failed: {}", why.to_string())),
            Ok(rows) => try_from_vec(rows, "files"),
        }
    }

    async fn get_file (&self, filename: String) -> Result<OnetimeFile, MyError>  {
        match self.client().await?.query_one(
            format!(
                "SELECT {}, {}, {}, {} FROM {}.{} WHERE {} = $1",
                FIELD_FILENAME,
                FIELD_CONTENTS,
                FIELD_CREATED_AT,
                FIELD_UPDATED_AT,
                self.schema,
                self.files_table,
                FIELD_FILENAME,
            ).as_str(),
            &[
                &filename,
            ],
        ).await {
            Err(why) => Err(format!("Get file failed: {}", why.to_string())),
            Ok(row) => OnetimeFile::try_from(row),
        }
    }

    async fn add_link (&self, link: OnetimeLink) -> Result<bool, MyError> {
        match self.client().await?.execute(
            format!(
                "INSERT INTO {}.{} ({}, {}, {}, {}, {}, {}, {}) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                self.schema,
                self.links_table,
                FIELD_TOKEN,
                FIELD_FILENAME,
                FIELD_NOTE,
                FIELD_CREATED_AT,
                FIELD_EXPIRES_AT,
                FIELD_DOWNLOADED_AT,
                FIELD_IP_ADDRESS,
            ).as_str(),
            &[
                &link.token,
                &link.filename,
                &link.note,
                &link.created_at,
                &link.expires_at,
                &link.downloaded_at,
                &link.ip_address,
            ],
        ).await {
            Err(why) => Err(format!("Add link failed: {}", why.to_string())),
            Ok(_) => Ok(true)
        }
    }

    async fn list_links (&self) -> Result<Vec<OnetimeLink>, MyError> {
        match self.client().await?.query(
            format!(
                "SELECT {}, {}, {}, {}, {}, {}, {} FROM {}.{}",
                FIELD_TOKEN,
                FIELD_FILENAME,
                FIELD_NOTE,
                FIELD_CREATED_AT,
                FIELD_EXPIRES_AT,
                FIELD_DOWNLOADED_AT,
                FIELD_IP_ADDRESS,
                self.schema,
                self.links_table,
            ).as_str(),
            &[
            ],
        ).await {
            Err(why) => Err(format!("List links failed: {}", why.to_string())),
            Ok(rows) => try_from_vec(rows, "links"),
        }
    }

    async fn get_link (&self, token: String) -> Result<OnetimeLink, MyError> {
        match self.client().await?.query_one(
            format!(
                "SELECT {}, {}, {}, {}, {}, {}, {} FROM {}.{} WHERE {} = $1",
                FIELD_TOKEN,
                FIELD_FILENAME,
                FIELD_NOTE,
                FIELD_CREATED_AT,
                FIELD_EXPIRES_AT,
                FIELD_DOWNLOADED_AT,
                FIELD_IP_ADDRESS,
                self.schema,
                self.links_table,
                FIELD_TOKEN,
            ).as_str(),
            &[
                &token,
            ],
        ).await {
            Err(why) => Err(format!("Get link failed: {}", why.to_string())),
            Ok(row) => OnetimeLink::try_from(row),
        }
    }

    async fn mark_downloaded (&self, link: OnetimeLink, ip_address: String, downloaded_at: i64) -> Result<bool, MyError> {
        match self.client().await?.execute(
            format!(
                "UPDATE {}.{} SET {} = $1, {} = $2 WHERE {} = $3 AND {} IS NULL",
                self.schema,
                self.links_table,
                FIELD_DOWNLOADED_AT,
                FIELD_IP_ADDRESS,
                FIELD_TOKEN,
                FIELD_DOWNLOADED_AT,
            ).as_str(),
            &[
                &downloaded_at,
                &ip_address,
                &link.token,
            ],
        ).await {
            Err(why) => Err(format!("Mark downloaded update failed: {}", why.to_string())),
            Ok(update_count) => Ok(update_count == 0)
        }
    }

    async fn delete_file(&self, filename: String) -> Result<bool, MyError> {
        match self.client().await?.execute(
            format!(
                "DELETE FROM {}.{} WHERE {} = $1",
                self.schema,
                self.files_table,
                FIELD_FILENAME,
            ).as_str(),
            &[
                &filename,
            ],
        ).await {
            Err(why) => Err(format!("Delete file failed: {}", why.to_string())),
            Ok(update_count) => Ok(update_count == 0)
        }
    }

    async fn delete_link(&self, token: String) -> Result<bool, MyError> {
        match self.client().await?.execute(
            format!(
                "DELETE FROM {}.{} WHERE {} = $1",
                self.schema,
                self.links_table,
                FIELD_TOKEN,
            ).as_str(),
            &[
                &token,
            ],
        ).await {
            Err(why) => Err(format!("Delete link failed: {}", why.to_string())),
            Ok(update_count) => Ok(update_count == 0)
        }
    }
}
