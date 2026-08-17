# Extending Silk Chiffon

Silk Chiffon has four registration seams. Choose the seam from the data's behavior, not from its vendor: file formats interpret exact objects, storage backends turn locations into object-store capabilities, service inputs create DataFusion providers from non-file references, and service outputs consume a completed record-batch stream.

The executable must explicitly register every implementation. Registration is definition-time composition; binding happens once after the host parses the combined command.

The compile-checked [`extensions` example](../crates/silk-chiffon-core/examples/extensions.rs) shows the exact callback signatures and builds minimal file-format, service-input, and service-output definitions. Its file and service-input stubs return errors because a real extension supplies the reader, writer, inspector, or client behind those contracts; its service-output example drains a successful stream.

## Add a file format

A format crate exports one `pub fn definition() -> FormatDefinition`. The definition has a canonical name and display name and may claim aliases and extensions. It can provide any subset of four capabilities:

- `InputDetectorFn` performs content recognition and returns `Mismatch`, `Match(FormatInputVariant)`, or `Malformed`.
- `InputProviderFn<T>` receives a homogeneous `FileInputGroup`, the shared `SessionContext`, and typed transform state. It returns a `TableProvider` and does not perform storage routing.
- `SinkBinderFn<T>` receives command-wide sink configuration and typed transform state. It returns command-scoped `SinkBinding` state that opens `DataSink` values for prepared storage targets.
- `InspectorFn<T>` receives one exact `InputObject`, the presentation mode, and typed inspection state.

Use `TransformDefinition::with_args::<T>()` and `InspectionDefinition::with_args::<T>()` when a format contributes Clap arguments. Give each derived Clap group an explicit format-and-operation ID so separately registered argument structs cannot collide. The registry validates duplicate names, aliases, extensions, and CLI keys.

Detection and transform input are intentionally separate capabilities. A detector-only definition can appear in `detect`, while `TransformBindings::detect` skips definitions that cannot build providers. Formats should use the `InputObject` and `FileInputGroup` supplied by the host rather than reparsing URLs or reopening local paths.

`Mismatch` means the available evidence does not identify this format. `Malformed` means the format was identified but its structure is invalid. The future's outer error is for storage, cancellation, or other operational failure. The registry does not enforce a byte limit, so a detector should read only the prefix, footer, or other metadata needed for identification rather than scanning the dataset.

For a complete implementation, follow the public `definition()` and private module layout in [`silk-chiffon-format-arrow`](../crates/silk-chiffon-format-arrow/src/lib.rs). The format registration contract is exercised without private type imports in [`crates/silk-chiffon-core/tests/registration.rs`](../crates/silk-chiffon-core/tests/registration.rs).

## Add a storage backend

A backend returns one `StorageBackend`, normally from a public `backend()` function. Start with `StorageBackend::with_args::<T>()` for typed CLI state or `StorageBackend::without_args()` when no backend-specific arguments exist. The builder requires:

- a canonical backend name;
- one or more canonical URL schemes;
- `StorageAccess` describing read, write, or both;
- a `LocationValidator` for backend-specific authority and query rules;
- an `ObjectStoreCreatorFn` that creates one client for a canonical store-root cache key.

One backend may exclusively claim the bare-location route. That owner may also add bare-pattern mapping, but a pattern mapper is invalid without the same backend's exact bare-location mapper. Shared retry settings are opt-in. Backend output preparation is also optional and runs after the session has claimed a target but before a format can open it.

The object-store creator receives the scheme-and-authority root used for session caching; it is not the private URL that the core later registers with DataFusion. Return an `Arc<dyn ObjectStore>` and let `StorageSession` own cache reuse. Do not add DataFusion behavior to the storage crate.

The host creates a session by augmenting its Clap command through `StorageRegistry`, parsing the host command, and calling `create_session`. Each call gets fresh typed backend state, retry settings, object-store cache, target claims, and upload controls. Clones of that session share those command-scoped resources.

Start with the compile-checked [`backend` example](../crates/silk-chiffon-storage/examples/backend.rs), then use [`crates/silk-chiffon-storage/src/local.rs`](../crates/silk-chiffon-storage/src/local.rs) for bare mapping and output preparation or the GCS and S3 modules for typed cloud settings. Builder validation and lifecycle examples live in [`crates/silk-chiffon-storage/tests/registration.rs`](../crates/silk-chiffon-storage/tests/registration.rs).

## Add a service input

A service input is for a source that is not an object-store file. Export one `ServiceInputDefinition` built with `with_args::<T>(provider)` or `without_args(provider)`, then assign a canonical name and one or more URL schemes.

The provider function receives the raw exact reference, the command's shared `SessionContext`, and typed command state. It may perform asynchronous schema discovery and create reusable client or snapshot state before returning an `Arc<dyn TableProvider>`. File patterns are not routed to service inputs.

The returned provider owns DataFusion-facing schema, filter and projection pushdown, statistics, partitioning, and boundedness. Ongoing reads belong to the physical streams returned by `ExecutionPlan::execute`; dropping those streams must cancel RPCs and close their channels promptly. Do not detach long-lived read tasks during provider construction.

[`silk-chiffon-input-bigquery`](../crates/silk-chiffon-input-bigquery/src/lib.rs) is the production example. Its private provider and execution modules show schema discovery followed by demand-driven partition streams, while the root fake-service test proves the public registration seam end to end.

## Add a service output

A service output is an exact non-file target that consumes the pipeline's final `SendableRecordBatchStream`. Export one `ServiceOutputDefinition` built with `with_args::<T>(consumer)` or `without_args(consumer)`, then assign its canonical name and schemes.

The consumer receives the raw target, ownership of the final stream, and typed command state. It must drain the stream, finish or commit the service operation, and return only after the destination's durability boundary. It also owns cancellation and cleanup if the stream or remote operation fails. Service outputs do not use `StorageSession`, object-store target templates, or format sinks.

The compile-checked extension example drains a successful stream. A real consumer performs its remote commit after that loop and returns only when the commit is durable; stream or remote errors must run its service-specific cleanup before returning. The core [`service_routes` contract tests](../crates/silk-chiffon-core/tests/service_routes.rs) verify typed binding and consumption.

## Wire the implementation into the executable

Add the crate to the workspace and register its `definition()` or `backend()` in [`src/registration.rs`](../src/registration.rs). Add a feature when the implementation is optional, and ensure an omitted implementation contributes no schemes or CLI arguments. The application rejects duplicate input schemes, duplicate output schemes, and duplicate CLI keys before execution.

Update the relevant feature matrix, registration tests, generated CLI documentation, crate-level rustdoc, and this guide. Run `just verify` before opening a pull request. The current package-inventory check does not claim that internal crates can be published independently; they still need an explicit version and release policy.
