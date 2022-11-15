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
    quantity: u64,
    price_per: Decimal,
}


pub struct Order {
    id: Uuid,
    created: DateTime<Utc>,
    customer_id: Uuid, tax_percent: Decimal,
    products: Vec<OrderProduct>,
    discount_amount: Decimal,
}

pub fn generate_data(customer_count: u64, product_count: u64, order_count: u64, max_products: u64, export_parquet: bool) {
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

    // Create and write out Customers
    {
        let id_column: String = "id".to_string();
        let mut pk_col: Vec<Uuid> = Vec::new();
        let mut email_col: Vec<String> = Vec::new();
        let mut name_col: Vec<String> = Vec::new();
        let mut address_col: Vec<String> = Vec::new();
        let mut created_col: Vec<DateTime<Utc>> = Vec::new();

        for i in 0..customer_count {
            if i % 1_000_000 == 0 {
                let meta: TableMetaData = TableMetaData{
                    table_name: "customers".to_string(),
                    columns: 5,
                    rows: 0, // Not used yet
                };
                let mut data: HashMap<String, Column> = HashMap::new();
                data.insert(id_column.clone(), Column::Uuid(pk_col));
                data.insert("name".to_string(), Column::String(name_col));
                data.insert("email".to_string(), Column::String(email_col));
                data.insert("address".to_string(), Column::String(address_col));
                data.insert("created".to_string(), Column::DateTime(created_col));
                let customers_table: Table = Table::new(id_column.clone(), meta, data).unwrap();
                let _ = customers_table.write_data().unwrap();
                pk_col = Vec::new();
                email_col = Vec::new();
                name_col = Vec::new();
                address_col = Vec::new();
                created_col = Vec::new();
            }
            let id = Uuid::new_v4();
            pk_col.push(id);
            email_col.push(FreeEmailProvider(EN).fake());
            address_col.push( {
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
            });
            created_col.push(DateTimeBetween(Utc::now() - Duration::weeks(52), Utc::now()).fake());
            name_col.push({
                let mut temp_string = String::new();
                temp_string.push_str(FirstName(EN).fake());
                temp_string.push_str(" ");
                temp_string.push_str(LastName(EN).fake());
                temp_string
            });
            customer_ids.push(id);
        }
        let meta: TableMetaData = TableMetaData{
            table_name: "customers".to_string(),
            columns: 5,
            rows: 0, // Not used yet
        };
        let mut data: HashMap<String, Column> = HashMap::new();
        data.insert(id_column.clone(), Column::Uuid(pk_col));
        data.insert("name".to_string(), Column::String(name_col));
        data.insert("email".to_string(), Column::String(email_col));
        data.insert("address".to_string(), Column::String(address_col));
        data.insert("created".to_string(), Column::DateTime(created_col));
        let customers_table: Table = Table::new(id_column, meta, data).unwrap();
        let _ = customers_table.write_data().unwrap();
    }
    println!("Customer IDs: {:?}", customer_ids.len());

    // Create and write out Products
    {
        let id_column: String = "id".to_string();
        let mut pk_col: Vec<Uuid> = Vec::new();
        let mut short_code_col: Vec<String> = Vec::new();
        let mut display_name_col: Vec<String> = Vec::new();
        let mut description_col: Vec<String> = Vec::new();
        let mut price_col: Vec<Decimal> = Vec::new();
        let mut initial_sale_date_col: Vec<DateTime<Utc>> = Vec::new();

        for i in 0..product_count {
            if i % 1_000_000 == 0 {
                let meta: TableMetaData = TableMetaData{
                    table_name: "products".to_string(),
                    columns: 5,
                    rows: 0, // Not used yet
                };
                let mut data: HashMap<String, Column> = HashMap::new();
                data.insert(id_column.clone(), Column::Uuid(pk_col));
                data.insert("short_code".to_string(), Column::String(short_code_col));
                data.insert("display_name".to_string(), Column::String(display_name_col));
                data.insert("description".to_string(), Column::String(description_col));
                data.insert("price".to_string(), Column::Decimal(price_col));
                data.insert("initial_sale_date".to_string(), Column::DateTime(initial_sale_date_col));
                let products_table: Table = Table::new(id_column.clone(), meta, data).unwrap();
                let _ = products_table.write_data().unwrap();
                pk_col = Vec::new();
                short_code_col = Vec::new();
                display_name_col = Vec::new();
                description_col = Vec::new();
                price_col = Vec::new();
                initial_sale_date_col = Vec::new();
            }
            let id = Uuid::new_v4();
            pk_col.push(id);
            short_code_col.push({
                let n4: u32 = rng.gen();
                let numbers: String = n4.to_string();
                numbers
            });                                                                                                  
            initial_sale_date_col.push(DateTimeBetween(Utc::now() - Duration::weeks(52), Utc::now() - Duration::weeks(30)).fake());
            display_name_col.push(Buzzword(EN).fake());
            description_col.push( CatchPhase(EN).fake());
            price_col.push({
                let n8: i64 = rng.gen_range(1..99);
                Decimal::new((n8 * 100) + 99, 2)
            });
            product_ids.push(id);
        }
        let meta: TableMetaData = TableMetaData{
            table_name: "products".to_string(),
            columns: 5,
            rows: 0, // Not used yet
        };
        let mut data: HashMap<String, Column> = HashMap::new();
        data.insert(id_column.clone(), Column::Uuid(pk_col));
        data.insert("short_code".to_string(), Column::String(short_code_col));
        data.insert("display_name".to_string(), Column::String(display_name_col));
        data.insert("description".to_string(), Column::String(description_col));
        data.insert("price".to_string(), Column::Decimal(price_col));
        data.insert("initial_sale_date".to_string(), Column::DateTime(initial_sale_date_col));
        let products_table: Table = Table::new(id_column, meta, data).unwrap();
        let _ = products_table.write_data().unwrap();
    }
    println!("Product IDs: {:?}", product_ids.len());
    
    // Generate and write orders
    {
        let id_column: String = "id".to_string();
        let mut pk_col: Vec<Uuid> = Vec::new();
        let mut created_col: Vec<DateTime<Utc>> = Vec::new();
        let mut customer_id_col: Vec<Uuid> = Vec::new();
        let mut tax_percent_col: Vec<Decimal> = Vec::new();
        let mut discount_amount_col: Vec<Decimal> = Vec::new();

        // For the mapping table from orders to products on the order
        let mut order_id_col: Vec<Uuid> = Vec::new();
        let mut product_id_col: Vec<Uuid> = Vec::new();
        let mut quantity_col: Vec<u64> = Vec::new();
        let mut price_per_col: Vec<Decimal> = Vec::new();

        let mut count = 0;
        let products_slice = product_ids.as_slice();
        for i in 0..order_count {
            if i % 10_000_000 == 0 {
                let meta: TableMetaData = TableMetaData{
                    table_name: "orders".to_string(),
                    columns: 5,
                    rows: 0, // Not used yet
                };
                let mut data: HashMap<String, Column> = HashMap::new();
                data.insert(id_column.clone(), Column::Uuid(pk_col));
                data.insert("customer_id".to_string(), Column::Uuid(customer_id_col));
                data.insert("created".to_string(), Column::DateTime(created_col));
                data.insert("tax_percent".to_string(), Column::Decimal(tax_percent_col));
                data.insert("discount_amount".to_string(), Column::Decimal(discount_amount_col));
                let customers_table: Table = Table::new(id_column.clone(), meta, data).unwrap();
                let _ = customers_table.write_data().unwrap();
                pk_col = Vec::new();
                customer_id_col = Vec::new();
                created_col = Vec::new();
                tax_percent_col = Vec::new();
                discount_amount_col = Vec::new();
                let meta_mapper: TableMetaData = TableMetaData{
                    table_name: "order_products".to_string(),
                    columns: 4,
                    rows: 0, // Not used yet
                };
                let mut data_mapper: HashMap<String, Column> = HashMap::new();
                data_mapper.insert("order_id".to_string(), Column::Uuid(order_id_col));
                data_mapper.insert("product_id".to_string(), Column::Uuid(product_id_col));
                data_mapper.insert("quantity".to_string(), Column::UInt64(quantity_col));
                data_mapper.insert("price_per".to_string(), Column::Decimal(price_per_col));
                let order_products_table: Table = Table::new("order_id".to_string(), meta_mapper, data_mapper).unwrap();
                let _ = order_products_table.write_data().unwrap();
                order_id_col = Vec::new();
                product_id_col = Vec::new();
                quantity_col = Vec::new();
                price_per_col = Vec::new();
            }
            let id = Uuid::new_v4();
            pk_col.push(id);
            created_col.push(DateTimeBetween(Utc::now() - Duration::weeks(52), Utc::now() - Duration::weeks(30)).fake());
            customer_id_col.push(customer_ids[rng.gen_range(0..customer_count) as usize]);
            tax_percent_col.push(Decimal::new(rng.gen_range(30..95), 1));
            for product in attachable_products(products_slice, max_products) {
                order_id_col.push(id);
                product_id_col.push(product.product_id);
                quantity_col.push(product.quantity);
                price_per_col.push(product.price_per);
            }
            discount_amount_col.push(Decimal::new(rng.gen_range(1..199), 2));
            // We don't need to save order IDs, they aren't being correlated to anything
            //order_ids.push(order.id);
            count += 1;
            if count % 10_000_000 == 0 {
                println!("Orders hit {:?} generated", count); 
            }
        }

        let meta: TableMetaData = TableMetaData{
            table_name: "orders".to_string(),
            columns: 5,
            rows: 0, // Not used yet
        };
        let mut data: HashMap<String, Column> = HashMap::new();
        data.insert(id_column.clone(), Column::Uuid(pk_col));
        data.insert("customer_id".to_string(), Column::Uuid(customer_id_col));
        data.insert("created".to_string(), Column::DateTime(created_col));
        data.insert("tax_percent".to_string(), Column::Decimal(tax_percent_col));
        data.insert("discount_amount".to_string(), Column::Decimal(discount_amount_col));
        let customers_table: Table = Table::new(id_column, meta, data).unwrap();
        let _ = customers_table.write_data().unwrap();

        let meta_mapper: TableMetaData = TableMetaData{
            table_name: "order_products".to_string(),
            columns: 4,
            rows: 0, // Not used yet
        };
        let mut data_mapper: HashMap<String, Column> = HashMap::new();
        data_mapper.insert("order_id".to_string(), Column::Uuid(order_id_col));
        data_mapper.insert("product_id".to_string(), Column::Uuid(product_id_col));
        data_mapper.insert("quantity".to_string(), Column::UInt64(quantity_col));
        data_mapper.insert("price_per".to_string(), Column::Decimal(price_per_col));
        let order_products_table: Table = Table::new("order_id".to_string(), meta_mapper, data_mapper).unwrap();
        let _ = order_products_table.write_data().unwrap(); 
    }
    // println!("Order IDs: {:?}", order_ids.len());


    // Generate a directory and files for fun and testing before we refactor all this
    //let id_column: String = "my_pk".to_string();
    //let meta: TableMetaData = TableMetaData{
    //    table_name: "my_test_table".to_string(),
    //    columns: 2,
    //    rows: 1,
    //};
    //let mut pk_col: Vec<Uuid> = Vec::new();
    //pk_col.push(Uuid::new_v4());
    //let mut names_col: Vec<String> = Vec::new();
    //names_col.push("The Dude".to_string());
    //let mut data: HashMap<String, Column> = HashMap::new();
    //data.insert(id_column.clone(), Column::Uuid(pk_col));
    //data.insert("names".to_string(), Column::String(names_col));
    //let table: Table = Table::new(id_column, meta, data).unwrap();
    //table.insert_data();
}

fn attachable_products(product_ids: &[Uuid], max_products: u64) -> Vec<OrderProduct> {
    let mut products: Vec<OrderProduct> = Vec::new();
    let mut rng = SmallRng::from_entropy();

    for _ in 1..rng.gen_range(1..max_products) {
        let prod_id = product_ids[rng.gen_range(0..product_ids.len()) as usize].clone();
        products.push(OrderProduct {
            product_id: prod_id,
            quantity: rng.gen_range(1..10) as u64,
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
