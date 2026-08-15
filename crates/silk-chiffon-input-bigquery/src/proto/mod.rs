//! Protocol types generated from the repository's pinned Google API closure.

mod google {
    #[allow(clippy::all, dead_code, deprecated, missing_docs)]
    pub mod api {
        tonic::include_proto!("google.api");
    }

    pub mod cloud {
        pub mod bigquery {
            pub mod storage {
                #[allow(
                    clippy::all,
                    clippy::clone_on_ref_ptr,
                    dead_code,
                    deprecated,
                    missing_docs
                )]
                pub mod v1 {
                    tonic::include_proto!("google.cloud.bigquery.storage.v1");
                }
            }
        }
    }

    #[allow(clippy::all, dead_code, deprecated, missing_docs)]
    pub mod rpc {
        tonic::include_proto!("google.rpc");
    }
}

pub mod bigquery_storage {
    #[cfg(test)]
    pub use super::google::cloud::bigquery::storage::v1::{
        ArrowRecordBatch, ArrowSchema, ReadStream, SplitReadStreamRequest, SplitReadStreamResponse,
        big_query_read_server,
    };
    pub use super::google::cloud::bigquery::storage::v1::{
        ArrowSerializationOptions, CreateReadSessionRequest, DataFormat, ReadRowsRequest,
        ReadRowsResponse, ReadSession, StorageError, arrow_serialization_options,
        big_query_read_client, read_rows_response, read_session, storage_error,
    };
}

pub mod rpc {
    pub use super::google::rpc::{RetryInfo, Status};
}

#[cfg(test)]
pub use prost_types::Any;
