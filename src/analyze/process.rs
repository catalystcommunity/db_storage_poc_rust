use crate::datagen;
use datagen::constants::{DATA_DIRECTORY};
use fake::faker::time::raw::Date;
use uuid::{Uuid, Builder};
use chrono::{DateTime, Utc, TimeZone, Datelike, Duration};
use itertools::Itertools;
use rust_decimal::Decimal;
use std::{collections::{HashMap, BinaryHeap, HashSet}, io::Read, cmp::{Ord,Ordering}};
use std::fs::OpenOptions;
use std::mem;
use std::path::Path;


#[derive(Debug)]
pub struct OrderStats {
    pub orders_count: u64,
    pub min_products: u64,
    pub max_products: u64,
    pub min_total_price: u64,
    pub max_total_price: u64,
}

#[derive(Debug)]
pub struct OrderMeta {
    pub customer_id: Uuid,
    pub count_products: u8,
    pub product_kinds: u8,
    pub qty_products: u8,
    pub total_price: Decimal,
    pub discount: Decimal,
}

#[derive(Debug)]
pub struct CustomerMeta {
    pub count_orders: u8,
    pub product_kinds: u8,
    pub qty_products: u8,
    pub total_price: Decimal,
}

impl CustomerMeta {
    pub fn gen_with_orders(orders: u8) -> CustomerMeta {
        CustomerMeta {
            count_orders: orders, 
            product_kinds: 0, 
            qty_products: 0, 
            total_price: Decimal::new(0,0),
        }
    }
}

#[derive(Debug)]
pub struct UuidHeapCount {
    pub order: Uuid,
    pub count: u8,
}

impl Ord for UuidHeapCount {
    fn cmp(&self, other: &Self) -> Ordering{
        if self.count > other.count { return Ordering::Greater }
        if self.count < other.count { return Ordering::Less }
        Ordering::Equal
    }
    fn max(self, other: Self) -> Self { 
        if self.count >= other.count { return self }
        other
    }
    fn min(self, other: Self) -> Self { 
        if self.count <= other.count { return self }
        other
    }
}

impl PartialOrd for UuidHeapCount {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for UuidHeapCount {
    fn eq(&self, other: &Self) -> bool {
        self.count == other.count
    }
}

impl Eq for UuidHeapCount {}

#[derive(Debug)]
pub struct UuidHeapDecimal {
    pub order: Uuid,
    pub count: u8,
}

impl Ord for UuidHeapDecimal {
    fn cmp(&self, other: &Self) -> Ordering{
        if self.count > other.count { return Ordering::Greater }
        if self.count < other.count { return Ordering::Less }
        Ordering::Equal
    }
    fn max(self, other: Self) -> Self { 
        if self.count >= other.count { return self }
        other
    }
    fn min(self, other: Self) -> Self { 
        if self.count <= other.count { return self }
        other
    }
}

impl PartialOrd for UuidHeapDecimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for UuidHeapDecimal {
    fn eq(&self, other: &Self) -> bool {
        self.count == other.count
    }
}

impl Eq for UuidHeapDecimal {}

pub fn get_file_as_bytes(filepath: String) -> Vec<u8> {
    let mut file = OpenOptions::new()
        .read(true)
        .open(&filepath)
        .unwrap();
    let mut buffer: Vec<u8> = Vec::new();
    file.read_to_end(&mut buffer).unwrap();
    buffer
}

pub fn process_data() {
    // This is all going to be hardcoded because I don't want to build a metadata file format for
    // this PoC, though all of that is trivial to determine at runtime dynamically.

    let customers_dir = DATA_DIRECTORY.to_owned()+"customers/";
    let orders_dir = DATA_DIRECTORY.to_owned()+"orders/";
    let products_dir = DATA_DIRECTORY.to_owned()+"products/";
    let order_products_dir = DATA_DIRECTORY.to_owned()+"order_products/";

    // Only use the current month if we're over 3 days into it
    let last_month = match Utc::now().date_naive().day() <= 3 {
        true => { 
            let last_month = Utc::now() - Duration::weeks(1);
            Utc.ymd(last_month.date_naive().year(), last_month.date_naive().month(), 1).and_hms(0,0,0) 
        },
        _ => { 
            Utc.ymd(Utc::now().date_naive().year(), Utc::now().date_naive().month(), 1).and_hms(0,0,0) 
        }
    };

    let mut order_metadata: HashMap<Uuid, OrderMeta> = HashMap::new();
    let mut customer_metadata: HashMap<Uuid, CustomerMeta> = HashMap::new();

    let mut bytes_scanned: u64 = 0;
    let mut time_start: DateTime<Utc> = Utc::now();

    let mut customer_count: u64 = 0;
    let mut orders_count: u64 = 0;
    let mut customer_purchases_last_month: u64 = 0;
    let mut customers_last_month: HashSet<Uuid>= HashSet::new();
    let mut orders_per_month: HashMap<DateTime<Utc>, u64> = HashMap::new();
    let mut min_orders_per_customer: u8 = u8::MAX;
    let mut max_orders_per_customer: u8 = 0;
    let mut min_quantity_per_order: u8 = u8::MAX;
    let mut max_quantity_per_order: u8 = 0;
    let mut total_quantity_per_order: u64 = 0;
    let mut min_kinds_per_order: u8 = u8::MAX;
    let mut max_kinds_per_order: u8 = 0;
    let mut total_kinds_per_order: u64 = 0;
    let mut min_total_per_order: Decimal = Decimal::new(i64::MAX,0);
    let mut max_total_per_order: Decimal = Decimal::new(0,0);
    let mut total_total_per_order: Decimal = Decimal::new(0,0);
    {
        println!("Beginning Customers Processing: {}", Utc::now());
        // How many customers do we have?
        let id_path: String = customers_dir + "id/";
        let id_size = mem::size_of::<Uuid>();
        let mut id_file_offset: usize = 0;
        let mut id_file_num: u64 = 0;
        while Path::new(&(id_path.to_owned() + "id_" + &format!("{:020}", id_file_num))).is_file() {
            let buffer = get_file_as_bytes(id_path.to_owned() + "id_" + &format!("{:020}", id_file_num));
            bytes_scanned += buffer.len() as u64;
            while id_file_offset < buffer.len() {
                let customer_id: Uuid = Builder::from_bytes(buffer[id_file_offset..id_file_offset+id_size].try_into().unwrap()).into_uuid();
                // if customer_count % 10000 == 0 {
                //     println!("Customer ID: {}", customer_id);
                // }
                customer_metadata.entry(customer_id).or_insert(CustomerMeta::gen_with_orders(0));
                customer_count += 1;
                id_file_offset += 16
            }
            id_file_num += 1;
            id_file_offset = 0;
        }
        // println!("Customers: {}", customer_count);
    }

    {
        println!("Beginning Orders Processing: {}", Utc::now());
        // Min/Max/Avg products per order
        // Min/Max/Avg total per order
        // Min/Max/Avg orders per customers
        let id_path: String = orders_dir.to_string() + "id/";
        let created_path: String = orders_dir.to_string() + "created/";
        let customer_id_path: String = orders_dir.to_string() + "customer_id/";
        let mut id_file_num: u64 = 0;
        let mut created_file_num: u64 = 0;
        let mut customer_id_file_num: u64 = 0;
        let mut id_file_offset: usize = 0;
        let mut created_file_offset: usize = 0;
        let mut customer_id_file_offset: usize = 0;
        let id_size = mem::size_of::<Uuid>();
        let created_size = mem::size_of::<i64>();
        let customer_id_size = mem::size_of::<Uuid>();

        while Path::new(&(id_path.to_owned() + "id_" + &format!("{:020}", id_file_num))).is_file() {
            let mut id_file_path = id_path.to_owned() + "id_" + &format!("{:020}", id_file_num);
            // println!("ID Path: {}", id_file_path);
            let mut id_buffer = get_file_as_bytes(id_file_path.to_owned());
            bytes_scanned += id_buffer.len() as u64;
            let mut created_file_path = created_path.to_owned() + "created_" + &format!("{:020}", created_file_num);
            // println!("Created Path: {}", created_file_path);
            let mut created_buffer: Vec<u8> = get_file_as_bytes(created_file_path.to_owned());
            bytes_scanned += created_buffer.len() as u64;
            let mut customer_id_file_path = customer_id_path.to_owned() + "customer_id_" + &format!("{:020}", customer_id_file_num);
            // println!("Customer ID Path: {}", customer_id_file_path);
            let mut customer_id_buffer: Vec<u8> = get_file_as_bytes(customer_id_file_path.to_owned());
            bytes_scanned += customer_id_buffer.len() as u64;
            while id_file_offset < id_buffer.len() {
                // println!("Created size: {}", ((row_num * created_size) - (created_prev_files_total)));
                // println!("Created Reader pos: {}", created_file.seek(SeekFrom::Current (0)).unwrap());
                // println!("Created filesize: {}", (&created_file.metadata().unwrap()).len());
                // if row_num % 100_000 == 0 {
                //     println!("Orders row: {}", row_num);
                // }
                if created_file_offset >= created_buffer.len() {
                    created_file_num += 1;
                    created_file_offset = 0;
                    created_file_path = id_path.to_owned() + "id_" + &format!("{:020}", created_file_num);
                    // println!("Created Path: {}", created_file_path);

                    created_buffer = get_file_as_bytes(created_file_path.to_owned());
                    bytes_scanned += created_buffer.len() as u64;
                }
                if customer_id_file_offset >= customer_id_buffer.len() {
                    customer_id_file_num += 1;
                    customer_id_file_offset = 0;
                    customer_id_file_path = id_path.to_owned() + "id_" + &format!("{:020}", customer_id_file_num);
                    // println!("Customer ID Path: {}", customer_id_file_path);

                    customer_id_buffer = get_file_as_bytes(customer_id_file_path.to_owned());
                    bytes_scanned += customer_id_buffer.len() as u64;
                }
                // Now increment customer order counts
                let order_id = Builder::from_bytes(id_buffer[id_file_offset..id_file_offset+id_size].try_into().unwrap()).into_uuid();
                id_file_offset += id_size;
                let customer_id = Builder::from_bytes(customer_id_buffer[customer_id_file_offset..customer_id_file_offset+customer_id_size].try_into().unwrap()).into_uuid();
                order_metadata.entry(order_id).or_insert(OrderMeta {
                    customer_id: customer_id, 
                    count_products: 0, 
                    product_kinds: 0, 
                    qty_products: 0, 
                    total_price: Decimal::new(0,0), 
                    discount: Decimal::new(0,0), 
                });
                customer_id_file_offset += customer_id_size;
                customer_metadata.entry(customer_id).and_modify(|meta| meta.count_orders += 1).or_insert(CustomerMeta::gen_with_orders(1));
                // Now increment month order counts
                let milli_bytes = &mut created_buffer[created_file_offset..created_file_offset+created_size];
                let millis = i64::from_le_bytes(milli_bytes.try_into().unwrap());
                created_file_offset += created_size;
                let datetime = Utc.timestamp_millis(millis);
                let month = Utc.ymd(datetime.date_naive().year(), datetime.date_naive().month(), 1).and_hms(0,0,0);
                // println!("Created month: {}", month);
                orders_per_month.entry(month).and_modify(|counter| *counter += 1).or_insert(1);
                if month == last_month {
                    customer_purchases_last_month += 1;
                    customers_last_month.insert(customer_id);
                }
                orders_count += 1;
            }
            id_file_num += 1;
            id_file_offset = 0;
        }
    }

    {
        println!("Beginning OrderProducts Processing: {}", Utc::now());
        // Min/Max/Avg products per order
        // Min/Max/Avg total per order
        // Min/Max/Avg orders per customers
        let order_id_path: String = order_products_dir.to_string() + "order_id/";
        let price_per_path: String = order_products_dir.to_string() + "price_per/";
        let quantity_path: String = order_products_dir.to_string() + "quantity/";
        let mut order_id_file_num: u64 = 0;
        let mut price_per_file_num: u64 = 0;
        let mut quantity_file_num: u64 = 0;
        let mut order_id_file_offset: usize = 0;
        let mut price_per_file_offset: usize = 0;
        let mut quantity_file_offset: usize = 0;
        let order_id_size = mem::size_of::<Uuid>();
        let price_per_size = mem::size_of::<Decimal>();
        let quantity_size = mem::size_of::<u64>();

        while Path::new(&(order_id_path.to_owned() + "order_id_" + &format!("{:020}", order_id_file_num))).is_file() {
            let mut order_id_file_path = order_id_path.to_owned() + "order_id_" + &format!("{:020}", order_id_file_num);
            let mut order_id_buffer = get_file_as_bytes(order_id_file_path.to_owned());
            bytes_scanned += order_id_buffer.len() as u64;
            let mut price_per_file_path = price_per_path.to_owned() + "price_per_" + &format!("{:020}", price_per_file_num);
            let mut price_per_buffer: Vec<u8> = get_file_as_bytes(price_per_file_path.to_owned());
            bytes_scanned += price_per_buffer.len() as u64;
            let mut quantity_file_path = quantity_path.to_owned() + "quantity_" + &format!("{:020}", quantity_file_offset);
            let mut quantity_buffer: Vec<u8> = get_file_as_bytes(quantity_file_path.to_owned());
            bytes_scanned += quantity_buffer.len() as u64;
            while order_id_file_offset < order_id_buffer.len() {
                // println!("Created size: {}", ((row_num * created_size) - (created_prev_files_total)));
                // println!("Created Reader pos: {}", created_file.seek(SeekFrom::Current (0)).unwrap());
                // println!("Created filesize: {}", (&created_file.metadata().unwrap()).len());
                // if row_num % 100_000 == 0 {
                //     println!("Order_Products row: {}", row_num);
                // }
                if price_per_file_offset >= price_per_buffer.len() {
                    price_per_file_num += 1;
                    price_per_file_offset = 0;
                    price_per_file_path = price_per_path.to_owned() + "id_" + &format!("{:020}", price_per_file_num);
                    // println!("Created Path: {}", price_per_file_path);

                    price_per_buffer = get_file_as_bytes(price_per_file_path.to_owned());
                    bytes_scanned += price_per_buffer.len() as u64;
                }
                if quantity_file_offset >= quantity_buffer.len() {
                    quantity_file_num += 1;
                    quantity_file_offset = 0;
                    quantity_file_path = quantity_path.to_owned() + "id_" + &format!("{:020}", quantity_file_num);
                    // println!("Customer ID Path: {}", quantity_file_path);

                    quantity_buffer = get_file_as_bytes(quantity_file_path.to_owned());
                    bytes_scanned += quantity_buffer.len() as u64;
                }
                // Now increment customer order counts
                let order_id = Builder::from_bytes(order_id_buffer[order_id_file_offset..order_id_file_offset+order_id_size].try_into().unwrap()).into_uuid();
                order_id_file_offset += order_id_size;
                let quantity = u64::from_le_bytes(quantity_buffer[quantity_file_offset..quantity_file_offset+quantity_size].try_into().unwrap());
                order_metadata.entry(order_id).and_modify(|val| val.qty_products += quantity as u8);
                order_metadata.entry(order_id).and_modify(|val| val.product_kinds += 1 as u8);
                quantity_file_offset += quantity_size;
                let price_per = Decimal::deserialize(price_per_buffer[price_per_file_offset..price_per_file_offset+price_per_size].try_into().unwrap());
                order_metadata.entry(order_id).and_modify(|val| val.total_price += price_per * Decimal::new(quantity as i64, 0));
                price_per_file_offset += price_per_size;

            }
            order_id_file_num += 1;
            order_id_file_offset = 0;
        }
    }

    println!("Final Tallying: {}", Utc::now());
    // let mut top_ten_products: BinaryHeap<UuidHeapCount> = BinaryHeap::new();
    for (_, order_meta) in order_metadata.iter() {
        if order_meta.qty_products > 0 && order_meta.qty_products < min_quantity_per_order { min_quantity_per_order = order_meta.qty_products}
        if order_meta.qty_products > max_quantity_per_order { max_quantity_per_order = order_meta.qty_products}
        total_quantity_per_order += order_meta.qty_products as u64;
        if order_meta.product_kinds > 0 && order_meta.product_kinds < min_kinds_per_order { min_kinds_per_order = order_meta.product_kinds}
        if order_meta.product_kinds > max_kinds_per_order { max_kinds_per_order = order_meta.product_kinds}
        total_kinds_per_order += order_meta.product_kinds as u64;
        if order_meta.total_price > Decimal::ZERO && order_meta.total_price < min_total_per_order { min_total_per_order = order_meta.total_price}
        if order_meta.total_price > max_total_per_order { max_total_per_order = order_meta.total_price}
        total_total_per_order += order_meta.total_price;
    }
    for (_, customer_meta) in customer_metadata.iter() {
        if customer_meta.count_orders > 0 && customer_meta.count_orders < min_orders_per_customer { min_orders_per_customer = customer_meta.count_orders}
        if customer_meta.count_orders > max_orders_per_customer { max_orders_per_customer = customer_meta.count_orders}
    }
     
    // TODO: Things we want to analyze:
    // - How many customers do we have?
    // - How many purchased in the last month?
    // - Min/Max/Avg products per order
    // - Min/Max/Avg total per order
    // - Min/Max/Avg orders per customers
    // - Orders per month for the last year
    // - Top ten selling products per count and per appearing on orders
    let time_spent = (Utc::now() - time_start).num_milliseconds();
    println!("Analysis complete: {}", Utc::now());
    println!("Time Spent: {:.4?}", (time_spent  as f64 / 1000.0));
    println!("Bytes Scanned: {}", bytes_scanned);
    println!("Bytes per second: {:#?}", bytes_scanned / (time_spent / 1000) as u64);
    
    let last_months_orders = orders_per_month.get(&last_month).unwrap_or(&0);
    println!("Customers: {}", customer_count);
    println!("Orders Last Month: {:#?}", last_months_orders);
    println!("Customer Purchases Last Month: {:#?}", customer_purchases_last_month);
    println!("Unique Customers Last Month: {:#?}", customers_last_month.len());
    println!("Min/Max/Avg total quantity per order: {}, {}, {}", min_quantity_per_order, max_quantity_per_order, (total_quantity_per_order/orders_count) as f64);
    println!("Min/Max/Avg product_kinds per order: {}, {}, {}", min_kinds_per_order, max_kinds_per_order, (total_kinds_per_order/orders_count) as f64);
    println!("Min/Max/Avg total per order: {:.2?}, {:.2?}, {:.2?}", min_total_per_order, max_total_per_order, (total_total_per_order/Decimal::new(orders_count as i64, 0)));
    println!("Min/Max/Avg orders per customer: {}, {}, {}", min_orders_per_customer, max_orders_per_customer, (orders_count/customer_count) as f64);
    println!("Orders: {:#?}", order_metadata.len());
    println!("Customers: {:#?}", customer_metadata.len());

    println!("Orders Per Month: {:#?}", orders_per_month.iter().sorted());
    
    //let id_paths = fs::read_dir(&id_path).unwrap();
    //let mut customer_count2: u64 = 0;
    //for path in id_paths {
    //    let pd = format!("{}", &path.as_ref().unwrap().path().display());
    //    customer_count2 += fs::metadata(&pd).unwrap().len()/mem::size_of::<Uuid>() as u64;
    //}
    //println!("Customers 2: {}", customer_count2);

}

pub fn process_average() {
    // This is all going to be hardcoded because I don't want to build a metadata file format for
    // this PoC, though all of that is trivial to determine at runtime dynamically.

    let order_products_dir = DATA_DIRECTORY.to_owned()+"order_products/";

    let mut bytes_scanned: u64 = 0;
    let mut time_start: DateTime<Utc> = Utc::now();

    let mut min_quantity_per_order: u64 = u64::MAX;
    let mut max_quantity_per_order: u64 = 0;
    let mut total_quantity_per_order: u64 = 0;
    let mut quantities_count: u64 = 0;

    {
        println!("Beginning OrderProducts Processing: {}", Utc::now());
        // Min/Max/Avg products per order
        // Min/Max/Avg total per order
        // Min/Max/Avg orders per customers
        let quantity_path: String = order_products_dir.to_string() + "quantity/";
        let mut quantity_file_num: u64 = 0;
        let mut quantity_file_offset: usize = 0;
        let quantity_size = mem::size_of::<u64>();

        while Path::new(&(quantity_path.to_owned() + "quantity_" + &format!("{:020}", quantity_file_num))).is_file() {
            let mut quantity_file_path = quantity_path.to_owned() + "quantity_" + &format!("{:020}", quantity_file_offset);
            let mut quantity_buffer: Vec<u8> = get_file_as_bytes(quantity_file_path.to_owned());
            bytes_scanned += quantity_buffer.len() as u64;
            while quantity_file_offset < quantity_buffer.len() {
                let quantity = u64::from_le_bytes(quantity_buffer[quantity_file_offset..quantity_file_offset+quantity_size].try_into().unwrap());
                quantity_file_offset += quantity_size;

                if quantity > 0 && quantity < min_quantity_per_order { min_quantity_per_order = quantity}
                if quantity > max_quantity_per_order { max_quantity_per_order = quantity}
                total_quantity_per_order += quantity;
                quantities_count += 1;
            }
    
            quantity_file_num += 1;
            quantity_file_offset = 0;
        }
    }

    if quantities_count == 0 {
        println!("No quantities scanned for whatever reason, maybe your data is missing?");
        return;
    }
    let time_spent = (Utc::now() - time_start).num_milliseconds();
    println!("Analysis complete: {}", Utc::now());
    println!("Time Spent: {:.4?}", (time_spent  as f64 / 1000.0));
    println!("Bytes Scanned: {}", bytes_scanned);
    println!("Bytes per second: {:#?}", bytes_scanned / (time_spent / 1000) as u64);
    
    println!("Quantities read: {}", quantities_count);
    println!("Min/Max/Avg total quantity per order: {}, {}, {:.2?}", min_quantity_per_order, max_quantity_per_order, (total_quantity_per_order as f64/quantities_count as f64));
 
}

