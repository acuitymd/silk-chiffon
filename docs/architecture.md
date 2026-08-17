# Architecture

Silk Chiffon is a DataFusion-forward command-line application with explicit registration seams for storage, file formats, service inputs, and service outputs. The executable composes concrete implementations; the core crates define contracts without importing those implementations.

## One transform

```text
CLI references
  -> application routing
     -> exact service reference -> service TableProvider
     -> file reference/pattern -> StorageSession -> InputObject
                               -> format detector -> file TableProvider
  -> DataFusion DataFrame and final physical plan
  -> PipelineExecution record-batch stream
     -> service output consumer
     -> file sink -> storage-owned ObjectUpload
```

The host builds registries before parsing the command. Each registry contributes its Clap arguments to the host command, then binds the parsed values once for that invocation. This keeps typed implementation settings paired with the callbacks that consume them without exposing those types through the registries.

## Input routing

An input URL scheme has one owner. Storage backends own file-like schemes such as `file`, `gs`, and `s3`; service-input definitions own service schemes such as `bqs`. A collision fails during application assembly.

File input is exact by the time a format sees it. `StorageSession` maps bare paths when enabled, validates the owning backend, caches an object-store client by canonical store root, observes metadata, and returns an `InputObject`. Pattern expansion produces exact objects before format detection. Exact operands preserve operand order and duplicates; pattern matches are deterministic but may be regrouped by store root, format, and container variant. Neither rule promises final row order.

Service inputs bypass storage and file-format detection. A bound service definition receives the raw exact reference and the command's shared `SessionContext`, then returns a `TableProvider`. Its physical plan owns boundedness, partitioning, statistics, pushdown, and read cancellation just like any other DataFusion provider.

## Format routing

A file-format definition can independently provide detection, input-provider creation, sink binding, and inspection. Extensions choose a preferred detector; bounded content detection confirms the format and container variant. Transform detection considers only definitions that can also create input providers, so a detector-only format can support `detect` without becoming a transform input.

The file route groups exact objects before asking the bound format to create a provider. Providers may delegate to DataFusion's native readers or implement a format-specific `FileSource`, but the shared core owns the exact-object provider boundary and unions providers by column name.

## Planning and execution

The pipeline builds a `DataFrame`, attaches the requested query and sort, then creates and validates the completed physical plan. DataFusion's physical-plan properties are the sole boundedness authority because operators can change boundedness. File sinks currently require a bounded final plan.

`PreparedPipeline::begin_execution` returns `PipelineExecution`, which implements `RecordBatchStream`. The execution retains the command session and spill resources until the stream is dropped. DataFusion execution tasks and source-specific work must remain owned by the returned stream so downstream failure or early drop propagates cancellation.

## Output routing

An output URL scheme also has one owner. A service-output binding consumes the final record-batch stream and is responsible for finishing its service operation before returning. A file output selects a format sink, renders one exact target per logical output, and asks `StorageSession` to claim and prepare each target.

Storage owns object durability. `ObjectUpload` chooses a single put or multipart upload, applies command-wide backpressure before part payloads are retained, and completes or aborts the remote operation. Format sinks own encoding but not remote upload lifetime. A target is reported only after the sink and upload finish successfully.

## Ownership rules

- The executable is the composition root and the only place that names every concrete implementation.
- `silk-chiffon-core` owns format, service, pipeline, and sink contracts, but no concrete format or cloud connector.
- `silk-chiffon-storage` owns location parsing, backend routing, object-store sessions, target claims, and uploads, but has no DataFusion dependency.
- A format crate exports one `definition()` and keeps its CLI state, readers, writers, and inspectors private.
- A service connector exports one `definition()` and keeps protocol and authentication machinery private. BigQuery has one explicitly feature-gated, doc-hidden protocol surface used only by the root fake-service integration test.
- Background work is owned by a stream, sink, or upload task. Cancellation must not depend on detached tasks eventually noticing that a command ended.

See [Extending Silk Chiffon](extending.md) for the concrete contracts and [Development](development.md) for the tests that guard these boundaries.
