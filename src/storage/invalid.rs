
use async_trait::async_trait;

use crate::models::{MyError, OnetimeFile, OnetimeLink, OnetimeStorage};


#[derive(Clone)]
pub struct Storage {
    pub error: String,
}

// https://github.com/dtolnay/async-trait#non-threadsafe-futures
#[async_trait(?Send)]
impl OnetimeStorage for Storage {
    async fn add_file (&self, _file: OnetimeFile) -> Result<bool, MyError> {
        Err(self.error.clone())
    }

    async fn list_files (&self) -> Result<Vec<OnetimeFile>, MyError>  {
        Err(self.error.clone())
    }

    async fn get_file (&self, _filename: String) -> Result<OnetimeFile, MyError>  {
        Err(self.error.clone())
    }

    async fn add_link (&self, _link: OnetimeLink) -> Result<bool, MyError> {
        Err(self.error.clone())
    }

    async fn list_links (&self) -> Result<Vec<OnetimeLink>, MyError> {
        Err(self.error.clone())
    }

    async fn get_link (&self, _token: String) -> Result<OnetimeLink, MyError> {
        Err(self.error.clone())
    }

    async fn mark_downloaded (&self, _link: OnetimeLink, _ip_address: String, _downloaded_at: i64) -> Result<bool, MyError> {
        Err(self.error.clone())
    }
}
