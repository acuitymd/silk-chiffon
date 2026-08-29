//! Cached object-store resolution for parsed locations.

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::local::LocalFileSystem;

use super::{
    StorageConfig,
    location::{ObjectLocation, ParsedLocation, StoreHandle, StoreKind},
    output::{ObjectOutput, OutputPolicy},
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum StoreKey {
    Local,
}

pub struct StorageContext {
    config: StorageConfig,
    stores: Mutex<HashMap<StoreKey, Arc<StoreHandle>>>,
}

impl StorageContext {
    pub fn new(config: StorageConfig) -> Result<Self> {
        Ok(Self {
            config,
            stores: Mutex::new(HashMap::new()),
        })
    }

    pub fn resolve(&self, value: &str) -> Result<ObjectLocation> {
        self.resolve_parsed(ParsedLocation::parse(value)?)
    }

    pub(crate) fn resolve_pattern(&self, value: &str) -> Result<ObjectLocation> {
        self.resolve_parsed(ParsedLocation::parse_pattern(value)?)
    }

    pub async fn create_output(&self, value: &str, policy: OutputPolicy) -> Result<ObjectOutput> {
        ObjectOutput::open(self.resolve(value)?, self.config, policy).await
    }

    pub(crate) async fn create_output_at(
        &self,
        destination: ObjectLocation,
        policy: OutputPolicy,
    ) -> Result<ObjectOutput> {
        ObjectOutput::open(destination, self.config, policy).await
    }

    fn resolve_parsed(&self, parsed: ParsedLocation) -> Result<ObjectLocation> {
        match parsed {
            ParsedLocation::Local {
                original,
                display,
                path,
            } => {
                let store = self.store(StoreKey::Local)?;
                Ok(ObjectLocation::new(original, display, path, store))
            }
        }
    }

    fn store(&self, key: StoreKey) -> Result<Arc<StoreHandle>> {
        let mut stores = self
            .stores
            .lock()
            .map_err(|_| anyhow::anyhow!("object-store cache lock is poisoned"))?;
        if let Some(store) = stores.get(&key) {
            return Ok(Arc::clone(store));
        }

        let handle = match &key {
            StoreKey::Local => StoreHandle::new(
                StoreKind::Local,
                ObjectStoreUrl::local_filesystem(),
                Arc::new(LocalFileSystem::new()),
            ),
        };
        let handle = Arc::new(handle);
        stores.insert(key, Arc::clone(&handle));
        Ok(handle)
    }
}

impl fmt::Debug for StorageContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageContext")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
