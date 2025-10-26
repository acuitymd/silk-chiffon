use crate::{
    io_strategies::{input_strategy::InputStrategy, output_strategy::OutputStrategy},
    operations::data_operation::DataOperation,
};

pub struct Pipeline {
    input_strategy: InputStrategy,
    operations: Vec<Box<dyn DataOperation>>,
    output_strategy: OutputStrategy,
}
