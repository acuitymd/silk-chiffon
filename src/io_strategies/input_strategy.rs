use crate::sources::data_source::DataSource;

pub enum InputStrategy {
    Single(Box<dyn DataSource>),
    Multiple(Vec<Box<dyn DataSource>>),
}
