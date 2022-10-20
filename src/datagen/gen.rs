use uuid::Uuid;
use chrono::{DateTime, Utc, Duration};
use rust_decimal::Decimal;
use fake::locales::EN;
use fake::faker::chrono::en::DateTimeBetween;
use fake::faker::lorem::raw::Word;
use fake::faker::address::raw::{CityName, ZipCode, StateAbbr, StreetSuffix};
use fake::faker::name::raw::{FirstName, LastName};
use fake::faker::internet::raw::FreeEmailProvider;
use fake::faker::company::raw::{Buzzword, CatchPhase};
use fake::Fake;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use super::dataset::{Table,Column,TableMetaData};


pub struct Customer {
    id: Uuid,
    email: String,
    created: DateTime<Utc>,
    name: String,
    address: String,
}

pub struct Product {
    id: Uuid,
    short_code: String,
    initial_sale_date: DateTime<Utc>,
    display_name: String,
    description: String,
    price: Decimal, }

pub struct OrderProduct {
    product_id: Uuid,
    quantity: u16,
    price_per: Decimal,
}


pub struct Order {
    id: Uuid,
    created: DateTime<Utc>,
    customer_id: Uuid, tax_percent: Decimal,
    products: Vec<OrderProduct>,
    discount_amount: Decimal,
}

pub fn Generate_data(customer_count: u64, product_count: u64, order_count: u64, max_products: u64, export_parquet: bool) {
    if export_parquet {
        println!("Currently exporting parquet is an ignored option.");
    } else {
        println!("You have chosen not to export to parquet, which we can't do yet anyway.");
    }

    const NUM_PLACES: u64 = 10;
    let mut rng = SmallRng::from_entropy();
    let mut customer_ids: Vec<Uuid> = Vec::new();
    let mut product_ids: Vec<Uuid> = Vec::new();
    let mut order_ids: Vec<Uuid> = Vec::new();
    let mut zip_codes: Vec<String> = Vec::new();
    let mut city_names: Vec<String> = Vec::new();
    let mut states: Vec<String> = Vec::new();

    for _ in 0..NUM_PLACES {
        zip_codes.push(ZipCode(EN).fake());
        city_names.push(CityName(EN).fake());
        states.push(StateAbbr(EN).fake());
    }

    for i in 0..customer_count {
        let customer: Customer = Customer{
            id: Uuid::new_v4(),
            email: FreeEmailProvider(EN).fake(),
            address: {
                let n1: u8 = rng.gen();
                let numbers: String = n1.to_string();
                let mut temp_string = String::new();
                temp_string.push_str(&numbers);
                temp_string.push_str(" ");
                temp_string.push_str(Word(EN).fake());
                temp_string.push_str(" ");
                temp_string.push_str(StreetSuffix(EN).fake());
                temp_string.push_str("\n");
                temp_string.push_str(&city_names[(i%NUM_PLACES) as usize]);
                temp_string.push_str(", ");
                temp_string.push_str(&states[(i%NUM_PLACES) as usize]);
                temp_string.push_str(" ");
                temp_string.push_str(&zip_codes[(i%NUM_PLACES) as usize]);
                temp_string
            },
            created: DateTimeBetween(Utc::now() - Duration::weeks(52), Utc::now()).fake(),
            name: {
                let mut temp_string = String::new();
                temp_string.push_str(FirstName(EN).fake());
                temp_string.push_str(" ");
                temp_string.push_str(LastName(EN).fake());
                temp_string
            },
        };
        customer_ids.push(customer.id);
    }
    println!("Customer IDs: {:?}", customer_ids.len());

    for _ in 0..product_count {
        let product: Product = Product {
            id: Uuid::new_v4(),
            short_code: {
                let n4: u32 = rng.gen();
                let numbers: String = n4.to_string();
                numbers
            },                                                                                                  
            initial_sale_date: DateTimeBetween(Utc::now() - Duration::weeks(52), Utc::now() - Duration::weeks(30)).fake(),                                                                                   
            display_name: Buzzword(EN).fake(),                                                                                               
            description: CatchPhase(EN).fake(),
            price: {
                let n8: i64 = rng.gen_range(1..99);
                Decimal::new((n8 * 100) + 99, 2)
            },
        };
        product_ids.push(product.id);
    }
    println!("Product IDs: {:?}", product_ids.len());
    
    let mut count = 0;
    let products_slice = product_ids.as_slice();
    for _ in 0..order_count {
        let order: Order = Order {
            id: Uuid::new_v4(),
            created: DateTimeBetween(Utc::now() - Duration::weeks(52), Utc::now() - Duration::weeks(30)).fake(),
            customer_id: customer_ids[rng.gen_range(0..customer_count) as usize],
            tax_percent: Decimal::new(rng.gen_range(30..95), 1),
            products: attachable_products(products_slice, max_products),
            discount_amount: Decimal::new(rng.gen_range(1..199), 2) ,
        };
        // We don't need to save order IDs, they aren't being correlated to anything
        //order_ids.push(order.id);
        count += 1;
        if count % 10_000_000 == 0 {
            println!("Orders hit {:?} generated", count); 
        }
    }
    // println!("Order IDs: {:?}", order_ids.len());


    // Generate a directory and files for fun and testing before we refactor all this
    let id_column: String = "my_pk".to_string();
    let meta: TableMetaData = TableMetaData{
        table_name: "my_test_table".to_string(),
        columns: 2,
        rows: 1,
    };
    let mut pk_col: Vec<Uuid> = Vec::new();
    pk_col.push(Uuid::new_v4());
    let mut names_col: Vec<String> = Vec::new();
    names_col.push("The Dude".to_string());
    let mut data: HashMap<String, Column> = HashMap::new();
    data.insert(id_column.clone(), Column::Uuid(pk_col));
    data.insert("names".to_string(), Column::String(names_col));
    let table: Table = Table::new(id_column, meta, data).unwrap();
    Table::insert_data(table);
}

fn attachable_products(product_ids: &[Uuid], max_products: u64) -> Vec<OrderProduct> {
    let mut products: Vec<OrderProduct> = Vec::new();
    let mut rng = SmallRng::from_entropy();

    for _ in 1..rng.gen_range(1..max_products) {
        let prod_id = product_ids[rng.gen_range(0..product_ids.len()) as usize].clone();
        products.push(OrderProduct {
            product_id: prod_id,
            quantity: rng.gen_range(1..10) as u16,
            price_per: {
                // Actually should just get the price per for the product
                let n8: i64 = rng.gen_range(1..99);
                Decimal::new((n8 * 100) + 99, 2)
            },
        });
    }
    //println!("Product Instances for this Order: {:?}", products.len();
    products
}
