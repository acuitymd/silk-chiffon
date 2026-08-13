const RELEASE_WORKFLOW: &str = include_str!("../.github/workflows/release.yml");

#[test]
fn release_cross_compiles_linux_arm_on_the_stable_x86_runner() {
    assert!(RELEASE_WORKFLOW.contains("fail-fast: false"));
    assert!(
        RELEASE_WORKFLOW
            .contains("target: aarch64-unknown-linux-gnu\n            runner: ubuntu-latest")
    );
    assert!(!RELEASE_WORKFLOW.contains("ubuntu-24.04-arm"));
    assert!(RELEASE_WORKFLOW.contains("shared-key: \"release-${{ matrix.target }}\""));
}

#[test]
fn release_validates_native_and_cross_compiled_binaries_separately() {
    assert!(RELEASE_WORKFLOW.contains("if: matrix.target != 'aarch64-unknown-linux-gnu'"));
    assert!(RELEASE_WORKFLOW.contains("if: matrix.target == 'aarch64-unknown-linux-gnu'"));
    assert!(RELEASE_WORKFLOW.contains("readelf --file-header"));
    assert!(RELEASE_WORKFLOW.contains("Machine:.*AArch64"));
}
