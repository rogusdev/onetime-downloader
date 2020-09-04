
/*

need to handle aws creds from env in docker

https://github.com/rusoto/rusoto/blob/master/AWS-CREDENTIALS.md
https://docs.rs/rusoto_credential/0.45.0/rusoto_credential/struct.ProfileProvider.html
https://docs.rs/rusoto_credential/0.45.0/src/rusoto_credential/profile.rs.html#53-62
https://docs.rs/rusoto_credential/0.45.0/rusoto_credential/struct.ChainProvider.html
https://www.rusoto.org/regions.html

*/

use std::collections::HashMap;
use bytes::{Bytes};
use maplit::hashmap;

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
use crate::objects::{OnetimeDownloaderConfig, OnetimeFile, OnetimeLink};


#[derive(Clone)]
pub struct DynamodbStorage {
    time_provider: Box<dyn TimeProvider>,
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

    pub fn from_env (time_provider: Box<dyn TimeProvider>) -> DynamodbStorage {
        DynamodbStorage {
            time_provider: time_provider,
            files_table: OnetimeDownloaderConfig::env_var_string("DDB_FILES_TABLE", String::from(Self::DEFAULT_TABLE_FILES)),
            links_table: OnetimeDownloaderConfig::env_var_string("DDB_LINKS_TABLE", String::from(Self::DEFAULT_TABLE_LINKS)),
            // https://docs.rs/rusoto_dynamodb/0.45.0/rusoto_dynamodb/
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

    pub async fn add_file (&self, file: OnetimeFile) -> Result<bool, String> {
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

    pub async fn list_files (&self) -> Result<Vec<OnetimeFile>, String>  {
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

    pub async fn get_file (&self, filename: String) -> Result<OnetimeFile, String>  {
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

    pub async fn add_link (&self, link: OnetimeLink) -> Result<bool, String> {
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
            // TODO: https://doc.rust-lang.org/reference/types/function-pointer.html -- maybe do collect_links and collect_files like this
            match Self::build_link(attributes) {
                Err(why) => return Err(format!("Failed collecting links: {}", why)),
                Ok(link) => vec.push(link),
            }
        }
        Ok(vec)
    }

    pub async fn list_links (&self) -> Result<Vec<OnetimeLink>, String> {
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

    pub async fn get_link (
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

    pub async fn mark_downloaded (
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
