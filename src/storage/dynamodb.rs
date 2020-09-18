
/*

need to handle aws creds from env in docker

https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
https://docs.rs/rusoto_credential/0.45.0/rusoto_credential/struct.ProfileProvider.html
https://docs.rs/rusoto_credential/0.45.0/src/rusoto_credential/profile.rs.html#53-62
https://docs.rs/rusoto_credential/0.45.0/rusoto_credential/struct.ChainProvider.html
https://www.rusoto.org/regions.html

*/

use std::collections::HashMap;
use std::convert::TryFrom;
use bytes::{Bytes};
use maplit::hashmap;
use async_trait::async_trait;

use rusoto_core::{Region};
use rusoto_dynamodb::{
    DynamoDb,
    DynamoDbClient,
    AttributeValue,
    GetItemInput,
    PutItemInput,
    ScanInput,
    DeleteItemInput,
};

use crate::time_provider::TimeProvider;
use crate::models::{MyError, OnetimeDownloaderConfig, OnetimeFile, OnetimeLink, OnetimeStorage};
use super::util::{try_from_vec};


const DEFAULT_TABLE_FILES: &'static str = "Onetime.Files";
const DEFAULT_TABLE_LINKS: &'static str = "Onetime.Links";

const FIELD_FILENAME: &'static str = "Filename";
const FIELD_CONTENTS: &'static str = "Contents";
const FIELD_CREATED_AT: &'static str = "CreatedAt";
const FIELD_UPDATED_AT: &'static str = "UpdatedAt";

const FIELD_TOKEN: &'static str = "Token";
const FIELD_NOTE: &'static str = "Note";
const FIELD_EXPIRES_AT: &'static str = "ExpiresAt";
const FIELD_DOWNLOADED_AT: &'static str = "DownloadedAt";
const FIELD_IP_ADDRESS: &'static str = "IpAddress";


#[derive(Clone)]
pub struct Storage {
    time_provider: Box<dyn TimeProvider>,
    files_table: String,
    links_table: String,
    client: DynamoDbClient,
}

// http://xion.io/post/code/rust-extension-traits.html
trait DdbAttributeValueExt {
    fn from_s (val: String) -> AttributeValue;
    fn from_n (val: i64) -> AttributeValue;
    fn from_b (val: Bytes) -> AttributeValue;
}

impl DdbAttributeValueExt for AttributeValue {
    fn from_s (val: String) -> AttributeValue {
        AttributeValue {
            s: Some(val),
            ..Default::default()
        }
    }

    fn from_n (val: i64) -> AttributeValue {
        AttributeValue {
            n: Some(val.to_string()),
            ..Default::default()
        }
    }

    fn from_b (val: Bytes) -> AttributeValue {
        AttributeValue {
            b: Some(val),
            ..Default::default()
        }
    }
}

trait RowExt {
    fn new_key (key: String, val: String) -> Self;
    fn filename_key (filename: String) -> Self;
    fn token_key (token: String) -> Self;

    fn get_s (&self, field: &String) -> Result<String, MyError>;
    fn get_os (&self, field: &String) -> Result<Option<String>, MyError>;
    fn get_b (&self, field: &String) -> Result<Bytes, MyError>;
    fn get_n (&self, field: &String) -> Result<i64, MyError>;
    fn get_on (&self, field: &String) -> Result<Option<i64>, MyError>;
}

type Row = HashMap<String, AttributeValue>;

impl RowExt for Row {
    fn new_key (key: String, val: String) -> Self {
        hashmap! {
            key => AttributeValue::from_s(val)
        }

        // let mut item = HashMap::new();
        // item.insert(key, AttributeValue::from_s(val));
    }

    fn filename_key (filename: String) -> Self {
        Self::new_key(FIELD_FILENAME.to_string(), filename)
    }

    fn token_key (token: String) -> Self {
        Self::new_key(FIELD_TOKEN.to_string(), token)
    }

    fn get_s (&self, field: &String) -> Result<String, MyError> {
        // clone because get returns Option<&V> (not Option<V>)
        //  and thus without clone, this attempts a move out of that (that fails to compile)
        //  https://doc.rust-lang.org/beta/std/collections/struct.HashMap.html#method.get
        self.get(field).ok_or(format!("Missing field {}", field))?.clone()
            .s.ok_or(format!("Empty field {}", field))
    }

    fn get_os (&self, field: &String) -> Result<Option<String>, MyError> {
        match self.get(field) {
            None => Ok(None),
            Some(val) => val.s.clone().ok_or(format!("Empty field {}", field)).map(|s| Some(s))
        }
    }

    fn get_b (&self, field: &String) -> Result<Bytes, MyError> {
        self.get(field).ok_or(format!("Missing field {}", field))?.clone()
            .b.ok_or(format!("Empty field {}", field))
            .map(|s| Bytes::from(s))
    }

    fn get_n (&self, field: &String) -> Result<i64, MyError> {
        self.get(field).ok_or(format!("Missing field {}", field))?.clone()
            .n.ok_or(format!("Empty field {}", field))?
            .parse::<i64>().map_err(|why| format!("Field {} is not a number {}", field, why))
    }

    fn get_on (&self, field: &String) -> Result<Option<i64>, MyError> {
        match self.get(field) {
            None => Ok(None),
            Some(val) => match val.n.clone() {
                None => Err(format!("Empty field {}", field)),
                Some(val) => match val.parse::<i64>() {
                    Err(why) => Err(format!("Field {} is not a number {}", field, why)),
                    Ok(val) => Ok(Some(val)),
                }
            }
        }
    }
}

impl TryFrom<Row> for OnetimeFile {
    type Error = MyError;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let filename = row.get_s(&FIELD_FILENAME.to_string())?;
        let contents = row.get_b(&FIELD_CONTENTS.to_string())?;
        let created_at = row.get_n(&FIELD_CREATED_AT.to_string())?;
        let updated_at = row.get_n(&FIELD_UPDATED_AT.to_string())?;

        Ok(Self {
            filename: filename,
            contents: contents,
            created_at: created_at,
            updated_at: updated_at,
        })
    }
}

impl TryFrom<Row> for OnetimeLink {
    type Error = MyError;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let token = row.get_s(&FIELD_TOKEN.to_string())?;
        let filename = row.get_s(&FIELD_FILENAME.to_string())?;
        let note = row.get_os(&FIELD_NOTE.to_string())?;
        let created_at = row.get_n(&FIELD_CREATED_AT.to_string())?;
        let expires_at = row.get_n(&FIELD_EXPIRES_AT.to_string())?;
        let downloaded_at = row.get_on(&FIELD_DOWNLOADED_AT.to_string())?;
        let ip_address = row.get_os(&FIELD_IP_ADDRESS.to_string())?;

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
    pub fn from_env (time_provider: Box<dyn TimeProvider>) -> Self {
        Self {
            time_provider: time_provider,
            files_table: OnetimeDownloaderConfig::env_var_string("DDB_FILES_TABLE", String::from(DEFAULT_TABLE_FILES)),
            links_table: OnetimeDownloaderConfig::env_var_string("DDB_LINKS_TABLE", String::from(DEFAULT_TABLE_LINKS)),
            // https://docs.rs/rusoto_dynamodb/0.45.0/rusoto_dynamodb/
            client: DynamoDbClient::new(Region::UsEast1),
        }
    }
}

// https://github.com/dtolnay/async-trait#non-threadsafe-futures
#[async_trait(?Send)]
impl OnetimeStorage for Storage {
    fn name(&self) -> &'static str {
        "Dynamodb"
    }

    async fn add_file (&self, file: OnetimeFile) -> Result<bool, MyError> {
        let item = hashmap! {
            FIELD_FILENAME.to_string() => AttributeValue::from_s(file.filename),
            FIELD_CONTENTS.to_string() => AttributeValue::from_b(file.contents),
            FIELD_CREATED_AT.to_string() => AttributeValue::from_n(file.created_at),
            FIELD_UPDATED_AT.to_string() => AttributeValue::from_n(file.updated_at),
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

    async fn list_files (&self) -> Result<Vec<OnetimeFile>, MyError>  {
        let projection_expression = [
            FIELD_FILENAME,
            FIELD_CONTENTS,
            FIELD_CREATED_AT,
            FIELD_UPDATED_AT,
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
                Some(rows) => try_from_vec(rows, "files"),
            }
        }
    }

    async fn get_file (&self, filename: String) -> Result<OnetimeFile, MyError>  {
        // https://www.rusoto.org/futures.html has example uses
        // ... maybe use https://docs.rs/crate/serde_dynamodb/0.6.0 ?
        let request = GetItemInput {
            key: Row::filename_key(filename),
            table_name: self.files_table.clone(),
            ..Default::default()
        };

        match self.client.get_item(request).await {
            Err(why) => Err(format!("Get file failed: {}", why.to_string())),
            Ok(output) => match output.item {
                None => Err("File not found".to_string()),
                Some(row) => OnetimeFile::try_from(row),
            }
        }
    }

    async fn add_link (&self, link: OnetimeLink) -> Result<bool, MyError> {
        let mut item = hashmap! {
            FIELD_TOKEN.to_string() => AttributeValue::from_s(link.token),
            FIELD_FILENAME.to_string() => AttributeValue::from_s(link.filename),
            FIELD_CREATED_AT.to_string() => AttributeValue::from_n(link.created_at),
            FIELD_EXPIRES_AT.to_string() => AttributeValue::from_n(link.expires_at),
        };
        if let Some(note) = link.note {
            item.insert(FIELD_NOTE.to_string(), AttributeValue::from_s(note));
        }
        if let Some(downloaded_at) = link.downloaded_at {
            item.insert(FIELD_DOWNLOADED_AT.to_string(), AttributeValue::from_n(downloaded_at));
        }
        if let Some(ip_address) = link.ip_address {
            item.insert(FIELD_IP_ADDRESS.to_string(), AttributeValue::from_s(ip_address));
        }

        let request = PutItemInput {
            item: item,
            table_name: self.links_table.clone(),
            ..Default::default()
        };

        match self.client.put_item(request).await {
            Err(why) => Err(format!("Add link failed: {}", why.to_string())),
            Ok(_) => Ok(true)
        }
    }

    async fn list_links (&self) -> Result<Vec<OnetimeLink>, MyError> {
        const TOKEN_SUBSTITUTE: &'static str = "#Token";

        let expression_attribute_names = hashmap! {
            TOKEN_SUBSTITUTE.to_string() => FIELD_TOKEN.to_string(),
        };

        let projection_expression = [
            TOKEN_SUBSTITUTE,
            FIELD_FILENAME,
            FIELD_NOTE,
            FIELD_CREATED_AT,
            FIELD_EXPIRES_AT,
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
    }

    async fn get_link (&self, token: String) -> Result<OnetimeLink, MyError> {
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
    }

    async fn mark_downloaded (&self, link: OnetimeLink, ip_address: String, downloaded_at: i64) -> Result<bool, MyError> {
        let mut item = hashmap! {
            FIELD_TOKEN.to_string() => AttributeValue::from_s(link.token),
            FIELD_FILENAME.to_string() => AttributeValue::from_s(link.filename),
            FIELD_CREATED_AT.to_string() => AttributeValue::from_n(link.created_at),
            FIELD_EXPIRES_AT.to_string() => AttributeValue::from_n(link.expires_at),
            FIELD_DOWNLOADED_AT.to_string() => AttributeValue::from_n(downloaded_at),
            FIELD_IP_ADDRESS.to_string() => AttributeValue::from_s(ip_address),
        };
        if let Some(note) = link.note {
            item.insert(FIELD_NOTE.to_string(), AttributeValue::from_s(note));
        }

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
    }

    async fn delete_file(&self, filename: String) -> Result<bool, MyError> {
        let request = DeleteItemInput {
            key: Row::filename_key(filename),
            table_name: self.files_table.clone(),
            ..Default::default()
        };

        match self.client.delete_item(request).await {
            Err(why) => Err(format!("Delete file failed: {}", why.to_string())),
            Ok(_) => Ok(true),
        }
    }

    async fn delete_link(&self, token: String) -> Result<bool, MyError> {
        let request = DeleteItemInput {
            key: Row::token_key(token),
            table_name: self.links_table.clone(),
            ..Default::default()
        };

        match self.client.delete_item(request).await {
            Err(why) => Err(format!("Delete link failed: {}", why.to_string())),
            Ok(_) => Ok(true),
        }
    }
}
