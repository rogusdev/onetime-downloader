
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
    ScanInput
};

use crate::time_provider::TimeProvider;
use crate::objects::{OnetimeDownloaderConfig, OnetimeFile, OnetimeLink, OnetimeStorage};


const DEFAULT_TABLE_FILES: &'static str = "Onetime.Files";
const DEFAULT_TABLE_LINKS: &'static str = "Onetime.Links";

const FIELD_FILENAME: &'static str = "Filename";
const FIELD_CONTENTS: &'static str = "Contents";
const FIELD_CREATED_AT: &'static str = "CreatedAt";
const FIELD_UPDATED_AT: &'static str = "UpdatedAt";

const FIELD_TOKEN: &'static str = "Token";
const FIELD_DOWNLOADED_AT: &'static str = "DownloadedAt";
const FIELD_IP_ADDRESS: &'static str = "IpAddress";


#[derive(Clone)]
pub struct DynamodbStorage {
    time_provider: Box<dyn TimeProvider>,
    files_table: String,
    links_table: String,
    client: DynamoDbClient,
}

// http://xion.io/post/code/rust-extension-traits.html
trait DdbAttributeValueExt {
    fn from_s (val: String) -> AttributeValue;
    fn from_n (val: u64) -> AttributeValue;
    fn from_b (val: Bytes) -> AttributeValue;
}

impl DdbAttributeValueExt for AttributeValue {
    fn from_s (val: String) -> AttributeValue {
        AttributeValue {
            s: Some(val),
            ..Default::default()
        }
    }

    fn from_n (val: u64) -> AttributeValue {
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

trait DdbAttributesExt {
    fn new_key (key: String, val: String) -> Self;
    fn get_s (&self, field: &String) -> Result<String, String>;
    fn get_os (&self, field: &String) -> Result<Option<String>, String>;
    fn get_b (&self, field: &String) -> Result<Bytes, String>;
    fn get_n (&self, field: &String) -> Result<u64, String>;
    fn get_on (&self, field: &String) -> Result<Option<u64>, String>;
}

type DdbAttributes = HashMap<String, AttributeValue>;

impl DdbAttributesExt for DdbAttributes {
    fn new_key (key: String, val: String) -> Self {
        hashmap! {
            key => AttributeValue::from_s(val)
        }

        // let mut item = HashMap::new();
        // item.insert(key, AttributeValue::from_s(val));
    }

    fn get_s (&self, field: &String) -> Result<String, String> {
        // clone because get returns Option<&V> (not Option<V>)
        //  and thus without clone, this attempts a move out of that (that fails to compile)
        //  https://doc.rust-lang.org/beta/std/collections/struct.HashMap.html#method.get
        self.get(field).ok_or(format!("Missing field {}", field))?.clone()
            .s.ok_or(format!("Empty field {}", field))
    }

    fn get_os (&self, field: &String) -> Result<Option<String>, String> {
        match self.get(field) {
            None => Ok(None),
            Some(val) => val.s.clone().ok_or(format!("Empty field {}", field)).map(|s| Some(s))
        }
    }

    fn get_b (&self, field: &String) -> Result<Bytes, String> {
        self.get(field).ok_or(format!("Missing field {}", field))?.clone()
            .b.ok_or(format!("Empty field {}", field))
            .map(|s| Bytes::from(s))
    }

    fn get_n (&self, field: &String) -> Result<u64, String> {
        self.get(field).ok_or(format!("Missing field {}", field))?.clone()
            .n.ok_or(format!("Empty field {}", field))?
            .parse::<u64>().map_err(|why| format!("Field {} is not a number {}", field, why))
    }

    fn get_on (&self, field: &String) -> Result<Option<u64>, String> {
        match self.get(field) {
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
}


impl TryFrom<DdbAttributes> for OnetimeFile {
    type Error = String;

    fn try_from(attributes: DdbAttributes) -> Result<Self, Self::Error> {
        let filename = attributes.get_s(&FIELD_FILENAME.to_string())?;
        let contents = attributes.get_b(&FIELD_CONTENTS.to_string())?;
        let created_at = attributes.get_n(&FIELD_CREATED_AT.to_string())?;
        let updated_at = attributes.get_n(&FIELD_UPDATED_AT.to_string())?;

        Ok(Self {
            filename: filename,
            contents: contents,
            created_at: created_at,
            updated_at: updated_at,
        })
    }
}

impl TryFrom<DdbAttributes> for OnetimeLink {
    type Error = String;

    fn try_from(attributes: DdbAttributes) -> Result<Self, Self::Error> {
        let token = attributes.get_s(&FIELD_TOKEN.to_string())?;
        let filename = attributes.get_s(&FIELD_FILENAME.to_string())?;
        let created_at = attributes.get_n(&FIELD_CREATED_AT.to_string())?;
        let downloaded_at = attributes.get_on(&FIELD_DOWNLOADED_AT.to_string())?;
        let ip_address = attributes.get_os(&FIELD_IP_ADDRESS.to_string())?;

        Ok(Self {
            token: token,
            filename: filename,
            created_at: created_at,
            downloaded_at: downloaded_at,
            ip_address: ip_address,
        })
    }
}

impl DynamodbStorage {
    pub fn from_env (time_provider: Box<dyn TimeProvider>) -> DynamodbStorage {
        DynamodbStorage {
            time_provider: time_provider,
            files_table: OnetimeDownloaderConfig::env_var_string("DDB_FILES_TABLE", String::from(DEFAULT_TABLE_FILES)),
            links_table: OnetimeDownloaderConfig::env_var_string("DDB_LINKS_TABLE", String::from(DEFAULT_TABLE_LINKS)),
            // https://docs.rs/rusoto_dynamodb/0.45.0/rusoto_dynamodb/
            client: DynamoDbClient::new(Region::UsEast1),
        }
    }

    fn collect_files (attributes_vec: Vec<DdbAttributes>) -> Result<Vec<OnetimeFile>, String>  {
        let mut vec = Vec::new();
        // https://stackoverflow.com/questions/34733811/what-is-the-difference-between-iter-and-into-iter
        for attributes in attributes_vec.into_iter() {
            match OnetimeFile::try_from(attributes) {
                Err(why) => return Err(format!("Failed collecting files: {}", why)),
                Ok(file) => vec.push(file),
            }
        }
        Ok(vec)
    }

    fn collect_links (attributes_vec: Vec<DdbAttributes>) -> Result<Vec<OnetimeLink>, String>  {
        let mut vec = Vec::new();
        for attributes in attributes_vec.into_iter() {
            // TODO: https://doc.rust-lang.org/reference/types/function-pointer.html -- maybe do collect_links and collect_files like this
            match OnetimeLink::try_from(attributes) {
                Err(why) => return Err(format!("Failed collecting links: {}", why)),
                Ok(link) => vec.push(link),
            }
        }
        Ok(vec)
    }

    fn filename_key (&self, filename: String) -> DdbAttributes {
        DdbAttributes::new_key(FIELD_FILENAME.to_string(), filename)
    }

    fn token_key (&self, token: String) -> DdbAttributes {
        DdbAttributes::new_key(FIELD_TOKEN.to_string(), token)
    }
}

// https://github.com/dtolnay/async-trait#non-threadsafe-futures
#[async_trait(?Send)]
impl OnetimeStorage for DynamodbStorage {
    async fn add_file (&self, file: OnetimeFile) -> Result<bool, String> {
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

    async fn list_files (&self) -> Result<Vec<OnetimeFile>, String>  {
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
                Some(attributes_vec) => Self::collect_files(attributes_vec),
            }
        }
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
                Some(attributes) => OnetimeFile::try_from(attributes),
            }
        }
    }

    async fn add_link (&self, link: OnetimeLink) -> Result<bool, String> {
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
    }

    async fn list_links (&self) -> Result<Vec<OnetimeLink>, String> {
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
                Some(attributes_vec) => Self::collect_links(attributes_vec),
            }
        }
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
                Some(attributes) => OnetimeLink::try_from(attributes),
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
                Some(attributes) => match OnetimeLink::try_from(attributes) {
                    Err(why) => Err(format!("Mark downloaded build failed: {}", why.to_string())),
                    Ok(link) => Ok(link.downloaded_at.is_some()),
                },
            }
        }
    }
}
