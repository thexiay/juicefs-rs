use bench::BenchOpts;
use clap::Parser;
use obj_bench::ObjBenchOpts;

mod bench;
mod obj_bench;

pub use obj_bench::juice_obj_bench;
pub use bench::juice_bench;

#[derive(Parser, Debug)]
#[command(rename_all = "lower")]
pub enum ToolCommands {
    Bench(BenchOpts),
    ObjBench(ObjBenchOpts),
}
