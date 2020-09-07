
use futures::executor::block_on;

use std::convert::TryFrom;
use bytes::{Bytes};
use async_trait::async_trait;

use deadpool_postgres::{Config, Client, Pool};
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
        let token = row.try_get(&FIELD_TOKEN).map_err(|why| format!("Could not get token! {}", why))?;
        let filename = row.try_get(&FIELD_FILENAME).map_err(|why| format!("Could not get filename! {}", why))?;
        let created_at = row.try_get(&FIELD_CREATED_AT).map_err(|why| format!("Could not get created_at! {}", why))?;
        let downloaded_at = row.try_get(&FIELD_DOWNLOADED_AT).map_err(|why| format!("Could not get downloaded_at! {}", why))?;
        let ip_address = row.try_get(&FIELD_IP_ADDRESS).map_err(|why| format!("Could not get ip_address! {}", why))?;

        Ok(Self {
            token: token,
            filename: filename,
            created_at: created_at,
            downloaded_at: Some(downloaded_at),
            ip_address: ip_address,
        })
    }
}

impl Storage {
    pub async fn init_tables (&self) -> Result<bool, MyError> {
        self.client().await?.batch_execute(
            format!(
                "
                --DROP SCHEMA IF EXISTS onetime CASCADE;
                CREATE SCHEMA IF NOT EXISTS onetime;

                CREATE TABLE IF NOT EXISTS {}.{} (
                    {} TEXT NOT NULL PRIMARY KEY,
                    {} BYTEA NOT NULL,
                    {} BIGINT NOT NULL,
                    {} BIGINT NOT NULL,
                );
                CREATE TABLE IF NOT EXISTS {}.{} (
                    {} TEXT NOT NULL PRIMARY KEY,
                    {} TEXT NOT NULL,
                    {} BIGINT NOT NULL,
                    {} BIGINT,
                    {} TEXT,
                );
                ",
                self.schema,
                self.files_table,
                FIELD_FILENAME,
                FIELD_CONTENTS,
                FIELD_CREATED_AT,
                FIELD_UPDATED_AT,
                self.schema,
                self.links_table,
                FIELD_TOKEN,
                FIELD_FILENAME,
                FIELD_CREATED_AT,
                FIELD_DOWNLOADED_AT,
                FIELD_IP_ADDRESS,
            ).as_str()
        ).await.map_err(|why| format!("Error initializing tables! {}", why))?;

        Ok(true)
    }

    pub fn from_env (time_provider: Box<dyn TimeProvider>) -> Result<Self, MyError> {
        let cfg = Config {
            host: Some(OnetimeDownloaderConfig::env_var_string("PG_HOST", String::from(DEFAULT_HOST))),
            port: Some(
                OnetimeDownloaderConfig::env_var_string("PG_PORT", String::from(DEFAULT_PORT))
                    .parse::<u16>().map_err(|why| format!("Port is not a valid number! {}", why))?
            ),
            user: Some(OnetimeDownloaderConfig::env_var_string("PG_USER", String::from(DEFAULT_USER))),
            password: Some(OnetimeDownloaderConfig::env_var_string("PG_PASS", String::from(DEFAULT_PASSWORD))),
            dbname: Some(OnetimeDownloaderConfig::env_var_string("PG_DBNAME", String::from(DEFAULT_DBNAME))),
            //manager: Option<ManagerConfig>,
            //pool: Option<PoolConfig>,
            ..Default::default()
        };

        let storage = Self {
            time_provider: time_provider,
            schema: OnetimeDownloaderConfig::env_var_string("PG_SCHEMA", String::from(DEFAULT_SCHEMA)),
            files_table: OnetimeDownloaderConfig::env_var_string("PG_FILES_TABLE", String::from(DEFAULT_TABLE_FILES)),
            links_table: OnetimeDownloaderConfig::env_var_string("PG_LINKS_TABLE", String::from(DEFAULT_TABLE_LINKS)),
            pool: cfg.create_pool(NoTls).map_err(|why| format!("Failed creating pool: {}", why))?,
            // TODO: replace w async postgres -- but have to choose between tokio, etc and fit w actix...
            // postgres mapping: https://github.com/Dowwie/tokio-postgres-mapper
            // actix example using postgres: https://github.com/actix/examples/tree/master/async_pg
            // client: Client::connect(
            //     // https://docs.rs/postgres/0.17.5/postgres/config/struct.Config.html
            //     OnetimeDownloaderConfig::env_var_string("PG_CONNECTION", String::from(DEFAULT_CONN_STR)).as_str(),
            //     NoTls
            // )?,
        };

        block_on(storage.init_tables())?;
        Ok(storage)
    }

    async fn client (&self) -> Result<Client, MyError> {
        self.pool.get().await.map_err(|why| format!("Failed creating pool: {}", why))
    }
}

// https://github.com/dtolnay/async-trait#non-threadsafe-futures
#[async_trait(?Send)]
impl OnetimeStorage for Storage {
    async fn add_file (&self, file: OnetimeFile) -> Result<bool, MyError> {
        match self.client().await?.execute(
            format!(
                "INSERT INTO {} ({}, {}, {}, {}) VALUES ($1, $2, $3, $4)",
                self.files_table,
                FIELD_FILENAME,
                FIELD_CONTENTS,
                FIELD_CREATED_AT,
                FIELD_UPDATED_AT,
            ).as_str(),
            &[
                &file.filename,
                &file.contents.as_ref(),
                &(file.created_at as i64),
                &(file.updated_at as i64),
            ],
        ).await {
            Err(why) => Err(format!("Add file failed: {}", why.to_string())),
            Ok(_) => Ok(true)
        }
    }

    async fn list_files (&self) -> Result<Vec<OnetimeFile>, MyError>  {
        match self.client().await?.query(
            format!(
                "SELECT {}, {}, {}, {} FROM {}",
                FIELD_FILENAME,
                FIELD_CONTENTS,
                FIELD_CREATED_AT,
                FIELD_UPDATED_AT,
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
                "SELECT {}, {}, {}, {} FROM {} WHERE filename = $1",
                FIELD_FILENAME,
                FIELD_CONTENTS,
                FIELD_CREATED_AT,
                FIELD_UPDATED_AT,
                self.files_table,
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
        Err("not yet implemented".to_string())
/*
        match self.client.execute(
            format!(
                "INSERT INTO {} ({}, {}, {}, {}, {}) VALUES ($1, $2, $3, $4, $5)",
                self.links_table,
                FIELD_TOKEN,
                FIELD_FILENAME,
                FIELD_CREATED_AT,
                FIELD_DOWNLOADED_AT,
                FIELD_IP_ADDRESS,
            ).as_str(),
            &[
                &file.filename,
                file.contents.as_ref(),
                &(file.created_at as i64),
                &(file.updated_at as i64),
            ],
        ) {
            Err(why) => Err(format!("Add link failed: {}", why.to_string())),
            Ok(_) => Ok(true)
        }

        let mut item = hashmap! {
            FIELD_TOKEN.to_string() => AttributeValue::from_s(link.token),
            FIELD_FILENAME.to_string() => AttributeValue::from_s(link.filename),
            FIELD_CREATED_AT.to_string() => AttributeValue::from_n(link.created_at),
        };
        if let Some(downloaded_at) = link.downloaded_at {
            item.insert(FIELD_DOWNLOADED_AT.to_string(), AttributeValue::from_n(downloaded_at));
        }
        if let Some(ip_address) = link.ip_address {
            item.insert(FIELD_IP_ADDRESS.to_string(), AttributeValue::from_s(ip_address));
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
*/
    }

    async fn list_links (&self) -> Result<Vec<OnetimeLink>, MyError> {
        Err("not yet implemented".to_string())
/*
        const TOKEN_SUBSTITUTE: &'static str = "#Token";

        let expression_attribute_names = hashmap! {
            TOKEN_SUBSTITUTE.to_string() => FIELD_TOKEN.to_string(),
        };

        let projection_expression = [
            TOKEN_SUBSTITUTE,
            FIELD_FILENAME,
            FIELD_CREATED_AT,
            FIELD_DOWNLOADED_AT,
            FIELD_IP_ADDRESS,
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
                Some(rows) => try_from_vec(rows, "links"),
            }
        }
*/
    }

    async fn get_link (&self, token: String) -> Result<OnetimeLink, MyError> {
        Err("not yet implemented".to_string())
/*
        // https://www.rusoto.org/futures.html has example uses
        // ... maybe use https://docs.rs/crate/serde_dynamodb/0.6.0 ?
        let request = GetItemInput {
            key: Row::token_key(token),
            table_name: self.links_table.clone(),
            ..Default::default()
        };

        match self.client.get_item(request).await {
            Err(why) => Err(format!("Get link failed: {}", why.to_string())),
            Ok(output) => match output.item {
                None => Err("Link not found".to_string()),
                Some(row) => OnetimeLink::try_from(row),
            }
        }
*/
    }

    async fn mark_downloaded (&self, link: OnetimeLink, ip_address: String, downloaded_at: i64) -> Result<bool, MyError> {
        Err("not yet implemented".to_string())
/*
        let item = hashmap! {
            FIELD_TOKEN.to_string() => AttributeValue::from_s(link.token),
            FIELD_FILENAME.to_string() => AttributeValue::from_s(link.filename),
            FIELD_CREATED_AT.to_string() => AttributeValue::from_n(link.created_at),
            FIELD_DOWNLOADED_AT.to_string() => AttributeValue::from_n(downloaded_at),
            FIELD_IP_ADDRESS.to_string() => AttributeValue::from_s(ip_address),
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
                Some(row) => match OnetimeLink::try_from(row) {
                    Err(why) => Err(format!("Mark downloaded build failed: {}", why.to_string())),
                    Ok(link) => Ok(link.downloaded_at.is_some()),
                },
            }
        }
*/
    }
}
