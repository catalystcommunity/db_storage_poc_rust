use uuid::{uuid, Uuid};
use chrono::{DateTime, TimeZone, NaiveDateTime, Utc};
use rust_decimal::Decimal;

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
    products: []OrderProduct,
    discount_amount: Decimal,
}
