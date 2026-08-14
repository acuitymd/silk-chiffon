//! Native DataFusion input-provider creation.

use std::sync::Arc;

use anyhow::Result;
use datafusion::{catalog::TableProvider, prelude::SessionContext};
use silk_chiffon_core::InputLeaf;
use vortex::session::VortexSession;
use vortex_datafusion::VortexFormat;

pub(crate) async fn create_provider(
    leaf: &InputLeaf,
    session: &SessionContext,
    vortex: &VortexSession,
) -> Result<Arc<dyn TableProvider>> {
    leaf.create_table_provider(session, Arc::new(VortexFormat::new(vortex.clone())))
        .await
}

#[cfg(test)]
mod tests {
    use datafusion::common::stats::Precision;
    use silk_chiffon_core::InputVariant;
    use vortex::{VortexSessionDefault, io::session::RuntimeSessionExt};

    use super::*;
    use crate::test_support::{guard, object_with, simple_batch, store, vortex_bytes};

    #[tokio::test]
    async fn native_provider_combines_files_and_executes_in_the_command_session() {
        let _guard = guard().await;
        let first = object_with(vortex_bytes(vec![simple_batch()]).await).await;
        let second = object_with(vortex_bytes(vec![simple_batch()]).await).await;
        store().reset_observation();
        let session = SessionContext::new();
        let leaf = InputLeaf::try_new(
            &session,
            &[first, second],
            InputVariant::named("file", "file"),
        )
        .unwrap();
        let vortex = vortex::session::VortexSession::default().with_tokio();

        let provider = create_provider(&leaf, &session, &vortex).await.unwrap();

        assert_eq!(provider.schema().fields().len(), 2);
        assert_eq!(provider.statistics().unwrap().num_rows, Precision::Exact(6));
        session.register_table("vortex_input", provider).unwrap();
        let batches = session
            .sql(
                "select name, id from vortex_input \
                 where id >= 2 order by id, name",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        assert_eq!(rows, 4);
        assert_eq!(batches[0].schema().field(0).name(), "name");
        assert!(!store().ranges().is_empty());
    }
}
