//! BigQuery Storage Read input definition for Silk Chiffon.
//!
//! The host composes this connector through [`definition`]. Protocol, authentication, session,
//! decoding, retry, and execution details remain private to this crate.

mod args;
mod auth;
mod decode;
mod execution;
#[cfg(test)]
mod fault;
mod http;
mod proto;
mod provider;
mod pushdown;
mod read_stream;
mod reference;
mod resources;
mod retry;
mod session;
mod snapshot;
mod transport;

use std::sync::Arc;

use anyhow::Result;
use datafusion::{catalog::TableProvider, prelude::SessionContext};
use futures::future::BoxFuture;
use silk_chiffon_core::ServiceInputDefinition;

use args::BigQueryInputArgs;

/// Protocol types used by Silk Chiffon's end-to-end fake service.
///
/// This module is available only through the non-default integration-test
/// feature. Applications compose the connector through [`definition`].
#[cfg(feature = "integration-test-support")]
#[doc(hidden)]
pub mod integration_test_support {
    pub use crate::proto::bigquery_storage::{
        ArrowRecordBatch, ArrowSchema, CreateReadSessionRequest, ReadRowsRequest, ReadRowsResponse,
        ReadSession, ReadStream, SplitReadStreamRequest, SplitReadStreamResponse,
        big_query_read_server, read_rows_response, read_session,
    };
}

/// Returns the BigQuery Storage Read service-input definition registered by a host application.
///
/// This also installs rustls's ring crypto provider as the process default when no provider has
/// already been installed. Repeated calls leave the existing default unchanged.
pub fn definition() -> ServiceInputDefinition {
    install_crypto_provider();
    ServiceInputDefinition::with_args::<BigQueryInputArgs>(create_provider)
        .name("bigquery-storage-read")
        .schemes(["bqs"])
        .build()
        .expect("the built-in BigQuery Storage Read definition must be valid")
}

fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

fn create_provider<'a>(
    reference: &'a str,
    session: &'a SessionContext,
    args: &'a BigQueryInputArgs,
) -> BoxFuture<'a, Result<Arc<dyn TableProvider>>> {
    Box::pin(provider::create_provider(reference, session, args))
}

#[cfg(test)]
mod tests {
    use clap::Command;

    use super::*;

    #[test]
    fn definition_exposes_only_the_canonical_registration() {
        let definition = definition();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
        assert_eq!(definition.name(), "bigquery-storage-read");
        assert_eq!(definition.schemes(), ["bqs"]);

        let help = definition
            .augment_args(Command::new("test"))
            .render_long_help()
            .to_string();
        assert!(help.contains("BigQuery Storage Read"));
        assert!(help.contains("--bqs-session-project"));
        assert!(help.contains("--bqs-max-response-bytes"));
    }
}
