# Silk Chiffon Arrow IPC format

This internal crate contributes Arrow IPC file and stream behavior to Silk Chiffon. Hosts register its immutable definition alongside other format definitions:

```rust
let registry = silk_chiffon_core::FormatRegistry::builder()
    .register(silk_chiffon_format_arrow::definition())
    .build()?;
# Ok::<(), silk_chiffon_core::FormatRegistryError>(())
```

The definition owns Arrow-specific CLI parsing, detection, DataFusion input-provider creation, output encoding, and inspection. Its concrete codec types stay private. Hosts interact with Arrow IPC through the shared format contract.
