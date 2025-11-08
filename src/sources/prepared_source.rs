use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use datafusion::{catalog::TableProvider, prelude::SessionContext};
use tempfile::NamedTempFile;

use crate::{
    sinks::{
        arrow::{ArrowSink, ArrowSinkOptions},
        data_sink::DataSink,
    },
    sources::{arrow_file::ArrowFileDataSource, data_source::DataSource},
};

pub enum PreparedSource {
    Direct {
        name: String,
        table_provider: Arc<dyn TableProvider>,
    },
    Materialized {
        name: String,
        table_provider: Arc<dyn TableProvider>,
        _temp_file: NamedTempFile,
    },
}

impl PreparedSource {
    pub async fn from_data_source(
        data_source: &Box<dyn DataSource>,
        ctx: &mut SessionContext,
    ) -> Result<Self> {
        if data_source.supports_table_provider() {
            Ok(Self::Direct {
                name: format!("prepared_direct_{}", data_source.name()),
                table_provider: data_source.as_table_provider(ctx).await?,
            })
        } else {
            let temp_file = NamedTempFile::with_suffix(".arrow")?;
            let temp_path = temp_file.path().to_path_buf();
            let schema = data_source.as_stream().await?.schema();
            let mut sink: Box<dyn DataSink> = Box::new(ArrowSink::create(
                temp_path.clone(),
                &schema,
                ArrowSinkOptions::default(),
            )?);
            sink.write_stream(data_source.as_stream().await?).await?;
            let arrow_data_source =
                ArrowFileDataSource::new(temp_path.to_str().unwrap().to_string());
            Ok(Self::Materialized {
                name: format!("prepared_materialized_{}", data_source.name()),
                table_provider: arrow_data_source.as_table_provider(ctx).await?,
                _temp_file: temp_file,
            })
        }
    }

    pub async fn from_data_source_with_working_directory(
        data_source: &Box<dyn DataSource>,
        ctx: &mut SessionContext,
        working_directory: String,
    ) -> Result<Self> {
        if data_source.supports_table_provider() {
            Ok(Self::Direct {
                name: format!("prepared_direct_{}", data_source.name()),
                table_provider: data_source.as_table_provider(ctx).await?,
            })
        } else {
            let temp_file = NamedTempFile::with_suffix_in(".arrow", working_directory)?;
            let temp_path = temp_file.path().to_path_buf();
            let schema = data_source.as_stream().await?.schema();
            let mut sink: Box<dyn DataSink> = Box::new(ArrowSink::create(
                temp_path.clone(),
                &schema,
                ArrowSinkOptions::default(),
            )?);
            sink.write_stream(data_source.as_stream().await?).await?;
            let arrow_data_source =
                ArrowFileDataSource::new(temp_path.to_str().unwrap().to_string());
            Ok(Self::Materialized {
                name: format!("prepared_materialized_{}", data_source.name()),
                table_provider: arrow_data_source.as_table_provider(ctx).await?,
                _temp_file: temp_file,
            })
        }
    }

    pub fn table_provider(&self) -> Arc<dyn TableProvider> {
        match self {
            PreparedSource::Direct { table_provider, .. } => table_provider.clone(),
            PreparedSource::Materialized { table_provider, .. } => table_provider.clone(),
        }
    }
}

#[async_trait]
impl DataSource for PreparedSource {
    fn name(&self) -> &str {
        match self {
            PreparedSource::Direct { name, .. } => name.as_str(),
            PreparedSource::Materialized { name, .. } => name.as_str(),
        }
    }

    async fn as_table_provider(&self, _ctx: &mut SessionContext) -> Result<Arc<dyn TableProvider>> {
        Ok(self.table_provider())
    }

    fn supports_table_provider(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::{arrow_file::ArrowFileDataSource, arrow_stream::ArrowStreamDataSource};
    use arrow::array::Int64Array;

    const TEST_ARROW_FILE_PATH: &str = "tests/files/people.file.arrow";
    const TEST_ARROW_STREAM_PATH: &str = "tests/files/people.stream.arrow";

    #[tokio::test]
    async fn test_from_data_source_with_table_provider_support() {
        let data_source: Box<dyn DataSource> =
            Box::new(ArrowFileDataSource::new(TEST_ARROW_FILE_PATH.to_string()));

        let mut ctx = SessionContext::new();

        let prepared_source = PreparedSource::from_data_source(&data_source, &mut ctx)
            .await
            .unwrap();

        assert!(matches!(prepared_source, PreparedSource::Direct { .. }));
    }

    #[tokio::test]
    async fn test_from_data_source_without_table_provider_support() {
        let data_source: Box<dyn DataSource> = Box::new(ArrowStreamDataSource::new(
            TEST_ARROW_STREAM_PATH.to_string(),
        ));

        assert!(!data_source.supports_table_provider());

        let mut ctx = SessionContext::new();

        let prepared_source = PreparedSource::from_data_source(&data_source, &mut ctx)
            .await
            .unwrap();

        let table_provider = match prepared_source {
            PreparedSource::Direct { .. } => panic!("Expected Materialized variant"),
            PreparedSource::Materialized { table_provider, .. } => table_provider,
        };

        assert!(!table_provider.schema().fields().is_empty());
    }

    #[tokio::test]
    async fn test_direct_source_can_query_data() {
        let data_source: Box<dyn DataSource> =
            Box::new(ArrowFileDataSource::new(TEST_ARROW_FILE_PATH.to_string()));

        let mut ctx = SessionContext::new();

        let prepared_source = PreparedSource::from_data_source(&data_source, &mut ctx)
            .await
            .unwrap();

        let table_provider = match prepared_source {
            PreparedSource::Direct { table_provider, .. } => table_provider,
            PreparedSource::Materialized { .. } => panic!("Expected Direct variant"),
        };

        let ctx = SessionContext::new();
        ctx.register_table("test_table", table_provider).unwrap();

        let df = ctx.sql("SELECT * FROM test_table LIMIT 1").await.unwrap();
        let batches = df.collect().await.unwrap();

        assert!(!batches.is_empty());
        assert!(batches[0].num_rows() > 0);
    }

    #[tokio::test]
    async fn test_materialized_source_can_query_data() {
        let data_source: Box<dyn DataSource> = Box::new(ArrowStreamDataSource::new(
            TEST_ARROW_STREAM_PATH.to_string(),
        ));

        let mut ctx = SessionContext::new();

        let prepared_source = PreparedSource::from_data_source(&data_source, &mut ctx)
            .await
            .unwrap();

        let table_provider = match prepared_source {
            PreparedSource::Direct { .. } => panic!("Expected Materialized variant"),
            PreparedSource::Materialized { table_provider, .. } => table_provider,
        };

        let ctx = SessionContext::new();
        ctx.register_table("test_table", table_provider).unwrap();

        let df = ctx.sql("SELECT * FROM test_table LIMIT 1").await.unwrap();
        let batches = df.collect().await.unwrap();

        assert!(!batches.is_empty());
        assert!(batches[0].num_rows() > 0);
    }

    #[tokio::test]
    async fn test_materialized_source_preserves_data() {
        let data_source: Box<dyn DataSource> = Box::new(ArrowStreamDataSource::new(
            TEST_ARROW_STREAM_PATH.to_string(),
        ));

        let mut ctx = SessionContext::new();

        let prepared_source = PreparedSource::from_data_source(&data_source, &mut ctx)
            .await
            .unwrap();

        let table_provider = match prepared_source {
            PreparedSource::Direct { .. } => panic!("Expected Materialized variant"),
            PreparedSource::Materialized { table_provider, .. } => table_provider,
        };

        let ctx = SessionContext::new();
        ctx.register_table("test_table", table_provider).unwrap();

        let df = ctx
            .sql("SELECT COUNT(*) as count FROM test_table")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        assert!(!batches.is_empty());
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 3);
    }
}
