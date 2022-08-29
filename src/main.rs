use clap::{Parser, ValueEnum};
use db_storage_poc_rust::{Customer, Order, Product, OrderProduct}

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// What mode to run the program in
    #[clap(arg_enum)]
    mode: Mode,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Mode {
    Fast,
    Slow,
}

fn main() {
   let args = Args::parse();

    match args.mode {
        Mode::Fast => {
            println!("Hare");
        }
        Mode::Slow => {
            println!("Tortoise");
        }
    }
}
