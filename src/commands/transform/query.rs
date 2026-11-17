use anyhow::{Result, anyhow};
use datafusion::prelude::{DataFrame, SessionContext};

const DEFAULT_TABLE_NAME: &str = "data";

pub(super) async fn apply_query(
    session: &SessionContext,
    input: DataFrame,
    query: &str,
) -> Result<DataFrame> {
    session.register_table(DEFAULT_TABLE_NAME, input.into_view())?;
    session.sql(query).await.map_err(|error| anyhow!(error))
}
