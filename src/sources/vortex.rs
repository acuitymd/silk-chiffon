use std::sync::Arc;

use anyhow::Result;
use datafusion::{catalog::TableProvider, prelude::SessionContext};
use silk_chiffon_storage::InputObject;
use vortex::{VortexSessionDefault, io::session::RuntimeSessionExt, session::VortexSession};
use vortex_datafusion::VortexFormat;

use crate::sources::file::native_file_provider;

pub(crate) async fn create_provider(
    objects: &[InputObject],
    session: &SessionContext,
) -> Result<Arc<dyn TableProvider>> {
    let vortex = VortexSession::default().with_tokio();
    native_file_provider(objects, session, Arc::new(VortexFormat::new(vortex))).await
}
