mod datagen;
//use datagen::dataset::{Customer, Order, Product, OrderProduct, generate_data};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(allow_negative_numbers = false)]
#[clap(propagate_version = true)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Adds files to myapp
    Generate { 
        //#[clap(short, long, default_value_t = 10_000_000)]
        #[clap(short, long, default_value_t = 1_000)]
        customer_count: u64, 
        //#[clap(short, long, default_value_t = 1_000_000)]
        #[clap(short, long, default_value_t = 1_000)]
        product_count: u64, 
        //#[clap(short, long, default_value_t = 1_000_000_000)]
        #[clap(short, long, default_value_t = 1_000)]
        order_count: u64, 
        #[clap(short, long, default_value_t = 10)]
        max_products: u64,
        #[clap(short, long)]
        export_parquet: bool,
    },
}


fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Generate { customer_count, product_count, order_count, max_products, export_parquet } => {
            println!("'db_storage_poc_rust generate' was used, customer_count is: {:?}\nmax_products is: {:?}", customer_count, max_products);
            datagen::gen::generate_data(*customer_count, *product_count, *order_count, *max_products, *export_parquet);
        }
    }
}

