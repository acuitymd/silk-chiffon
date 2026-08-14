use std::sync::Arc;

use anyhow::Result;
use datafusion::{catalog::TableProvider, prelude::SessionContext};
use silk_chiffon_core::InputLeaf;
use vortex::{VortexSessionDefault, io::session::RuntimeSessionExt, session::VortexSession};
use vortex_datafusion::VortexFormat;

pub(crate) async fn create_provider(
    leaf: &InputLeaf,
    session: &SessionContext,
) -> Result<Arc<dyn TableProvider>> {
    let vortex = VortexSession::default().with_tokio();
    leaf.create_table_provider(session, Arc::new(VortexFormat::new(vortex)))
        .await
}
