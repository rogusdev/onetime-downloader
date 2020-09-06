
use async_trait::async_trait;

use crate::objects::{OnetimeFile, OnetimeLink, OnetimeStorage};


#[derive(Clone)]
pub struct Storage {
    pub error: String,
}

// https://github.com/dtolnay/async-trait#non-threadsafe-futures
#[async_trait(?Send)]
impl OnetimeStorage for Storage {
    async fn add_file (&self, _file: OnetimeFile) -> Result<bool, String> {
        Err(self.error.clone())
    }

    async fn list_files (&self) -> Result<Vec<OnetimeFile>, String>  {
        Err(self.error.clone())
    }

    async fn get_file (&self, _filename: String) -> Result<OnetimeFile, String>  {
        Err(self.error.clone())
    }

    async fn add_link (&self, _link: OnetimeLink) -> Result<bool, String> {
        Err(self.error.clone())
    }

    async fn list_links (&self) -> Result<Vec<OnetimeLink>, String> {
        Err(self.error.clone())
    }

    async fn get_link (&self, _token: String) -> Result<OnetimeLink, String> {
        Err(self.error.clone())
    }

    async fn mark_downloaded (&self, _link: OnetimeLink, _ip_address: String, _downloaded_at: u64) -> Result<bool, String> {
        Err(self.error.clone())
    }
}
