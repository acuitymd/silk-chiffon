use anyhow::Result;
use clap::ValueEnum;
use silk_chiffon_core::FormatInputVariant;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
#[value(rename_all = "lowercase")]
pub(crate) enum IpcVariant {
    #[default]
    File,
    Stream,
}

impl IpcVariant {
    pub(crate) fn parse(variant: &FormatInputVariant) -> Result<Self> {
        match variant.name() {
            Some("file") => Ok(Self::File),
            Some("stream") => Ok(Self::Stream),
            other => anyhow::bail!("unknown Arrow IPC input variant {other:?}"),
        }
    }

    pub(crate) fn format_input_variant(self) -> FormatInputVariant {
        FormatInputVariant::named(self.canonical_name(), self.display_name())
    }

    pub(crate) const fn canonical_name(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Stream => "stream",
        }
    }

    pub(crate) const fn display_name(self) -> &'static str {
        self.canonical_name()
    }
}

impl std::fmt::Display for IpcVariant {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.canonical_name())
    }
}
