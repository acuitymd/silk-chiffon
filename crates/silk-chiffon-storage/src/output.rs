//! Policies applied before a format opens one output sink.

/// How target preparation treats an object observed before writing begins.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExistingOutput {
    /// Permit an object observed at the target.
    Allow,
    /// Reject an object observed by the advisory metadata request.
    RejectIfObserved,
}

/// Storage and backend policy for preparing one output target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutputPreparation {
    existing_output: ExistingOutput,
    create_parent_directories: bool,
}

impl OutputPreparation {
    /// Creates the policy for one target preparation request.
    pub const fn new(existing_output: ExistingOutput, create_parent_directories: bool) -> Self {
        Self {
            existing_output,
            create_parent_directories,
        }
    }

    /// Returns the advisory external-object policy.
    pub const fn existing_output(self) -> ExistingOutput {
        self.existing_output
    }

    /// Returns whether a backend should create missing parent directories.
    pub const fn create_parent_directories(self) -> bool {
        self.create_parent_directories
    }
}
