use db_storage_poc_rust::{Customer, Order, Product, OrderProduct};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Adds files to myapp
    Generate { 
        customer_count: Option<u64>, 
        product_count: Option<u64>, 
        order_count: Option<u64>, 
        max_products: Option<u64>,
        export_parquet: Option<bool>,
    },
}

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Generate { customer_count, product_count, order_count, max_products, export_parquet } => {
            println!("'db_storage_poc_rust generate' was used, customer_count is: {:?}", customer_count)
        }
    }
}

