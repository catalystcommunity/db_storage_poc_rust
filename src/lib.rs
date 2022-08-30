use uuid::{Uuid};
use chrono::{DateTime, Utc, Duration};
use rust_decimal::Decimal;
use fake::locales::EN;
use fake::faker::chrono::en::DateTimeBetween;
use fake::faker::lorem::raw::{Word};
use fake::faker::address::raw::{CityName, ZipCode, StateAbbr, StreetSuffix};
use fake::faker::name::raw::{FirstName, LastName};
use fake::faker::internet::raw::{FreeEmailProvider};
use fake::{Fake};
use rand::{Rng};

pub struct Customer {
    id: Uuid,
    email: String,
    created: DateTime<Utc>,
    name: String,
    address: String,
}

pub struct Product {
    id: Uuid,
    shortcode: String,
    initial_sale_date: DateTime<Utc>,
    display_name: String,
    description: String,
    price: Decimal,
}

pub struct OrderProduct {
    product_id: Uuid,
    quantity: u16,
    price_per: Decimal,
}


pub struct Order {
    id: Uuid,
    created: DateTime<Utc>,
    customer_id: Uuid,
    tax_percent: Decimal,
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
    let mut rng = rand::thread_rng();
    let mut customer_ids: Vec<Uuid> = Vec::new();
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
                let nums: String = n1.to_string();
                let mut temp_string = String::new();
                temp_string.push_str(&nums);
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
}
