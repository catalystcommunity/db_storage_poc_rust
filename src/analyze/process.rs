use crate::datagen;
use datagen::constants::{DATA_DIRECTORY};
use uuid::{Uuid, Builder};
use chrono::{DateTime, Utc, TimeZone, NaiveDate, Datelike};
use itertools::Itertools;
use rust_decimal::Decimal;
use std::{collections::HashMap, fs::File, io::{Read, BufReader}};
use std::fs;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom};
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

pub fn process_data() {
    // This is all going to be hardcoded because I don't want to build a metadata file format for
    // this PoC, though all of that is trivial to determine at runtime dynamically.
    
    // TODO: Things we want to analyze:
    // - How many customers do we have?
    // - How many purchased in the last month?
    // - Min/Max/Avg products per order
    // - Min/Max/Avg total per order
    // - Min/Max/Avg orders per customers
    // - Orders per month for the last year
    // - Top ten selling products per count and per appearing on orders

    let customers_dir = DATA_DIRECTORY.to_owned()+"customers/";
    let orders_dir = DATA_DIRECTORY.to_owned()+"orders/";
    let products_dir = DATA_DIRECTORY.to_owned()+"products/";
    let order_products_dir = DATA_DIRECTORY.to_owned()+"order_products/";

    let mut customer_count: u64 = 0;
    let mut customers_last_month: u64 = 0;
    let mut avg_orders_per_customer: f64 = 0.0;
    let mut min_orders_per_customer: u64 = 0;
    let mut max_orders_per_customer: u64 = 0;
    let mut orders_per_month: HashMap<DateTime<Utc>, u64> = HashMap::new();
    let mut top_products_count: HashMap<String, u64> = HashMap::new();
    let mut top_products_ordered: HashMap<String, u64> = HashMap::new();

    {
        // How many customers do we have?
        let id_path: String = customers_dir + "id/";
        let id_size: u64 = mem::size_of::<Uuid>() as u64;
        let mut id_file_num: u64 = 0;
        let mut row_num: u64 = 0;
        while Path::new(&(id_path.to_owned() + "id_" + &format!("{:020}", id_file_num))).is_file() {
            let file = OpenOptions::new()
                    .read(true)
                    .open(&(id_path.to_owned() + "id_" + &format!("{:020}", id_file_num)))
                    .unwrap();
            let mut reader = BufReader::new(&file);
            let mut buffer: Vec<u8> = vec![0_u8; id_size as usize];
            while (row_num * id_size) < file.metadata().unwrap().len() {
                reader.read_exact(&mut buffer).unwrap();
                if false && customer_count % 10000 == 0 {
                    println!("Customer ID: {}", Builder::from_bytes(buffer[0..16].try_into().unwrap()).into_uuid());
                }
                row_num += 1;
                customer_count += 1;
            }
            id_file_num += 1;
        }
        println!("Customers: {}", customer_count);
    }


    {
        // Min/Max/Avg products per order
        // Min/Max/Avg total per order
        // Min/Max/Avg orders per customers
        let id_path: String = orders_dir.to_string() + "id/";
        let created_path: String = orders_dir.to_string() + "created/";
        let customer_id_path: String = orders_dir.to_string() + "customer_id/";
        let mut id_file_num: u64 = 0;
        let mut created_file_num: u64 = 0;
        let mut customer_id_file_num: u64 = 0;
        let mut row_num: u64 = 0;
        let id_size: u64 = mem::size_of::<Uuid>() as u64;
        let created_size: u64 = mem::size_of::<i64>() as u64;
        let customer_id_size: u64 = mem::size_of::<Uuid>() as u64;
        let mut id_prev_files_total: u64 = 0;
        let mut created_prev_files_total: u64 = 0;
        let mut customer_id_prev_files_total: u64 = 0;

        let mut orders_per_month: HashMap<DateTime<Utc>, u64> = HashMap::new();
        let mut orders_per_customer: HashMap<Uuid, u64> = HashMap::new();
        let mut order_stats: OrderStats = OrderStats {
            orders_count: 0,
            min_products: 0,
            max_products: 0,
            min_total_price: 0,
            max_total_price: 0,
        };
        while Path::new(&(id_path.to_owned() + "id_" + &format!("{:020}", id_file_num))).is_file() {
            println!("ID Path: {}", &(id_path.to_owned() + "id_" + &format!("{:020}", id_file_num)));
            let mut id_file = OpenOptions::new()
                    .read(true)
                    .open(&(id_path.to_owned() + "id_" + &format!("{:020}", id_file_num)))
                    .unwrap();
            let mut id_buffer: Vec<u8> = vec![0_u8; id_size as usize];
            println!("Created Path: {}", &(created_path.to_owned() + "created_" + &format!("{:020}", created_file_num)));
            let mut created_file = OpenOptions::new()
                    .read(true)
                    .open(&(created_path.to_owned() + "created_" + &format!("{:020}", created_file_num)))
                    .unwrap();
            let mut created_buffer: Vec<u8> = vec![0_u8; created_size as usize];
            println!("Customer ID Path: {}", &(customer_id_path.to_owned() + "customer_id_" + &format!("{:020}", customer_id_file_num)));
            let mut customer_id_file = OpenOptions::new()
                    .read(true)
                    .open(&(customer_id_path.to_owned() + "customer_id_" + &format!("{:020}", customer_id_file_num)))
                    .unwrap();
            let mut customer_id_buffer: Vec<u8> = vec![0_u8; customer_id_size as usize];
            while ((row_num * id_size) - (id_prev_files_total)) < id_file.metadata().unwrap().len() {
                // println!("Created size: {}", ((row_num * created_size) - (created_prev_files_total)));
                // println!("Created Reader pos: {}", created_file.seek(SeekFrom::Current (0)).unwrap());
                // println!("Created filesize: {}", (&created_file.metadata().unwrap()).len());
                if row_num % 100_000 == 0 {
                    println!("Orders row: {}", row_num);
                }
                if ((row_num * created_size) - (created_prev_files_total)) >= (&created_file.metadata().unwrap()).len() {
                    created_prev_files_total += (&created_file.metadata().unwrap()).len();
                    created_file_num += 1;
                    println!("Created Path: {}", &(created_path.to_owned() + "created_" + &format!("{:020}", created_file_num)));
                    created_file = OpenOptions::new()
                        .read(true)
                        .open(&(created_path.to_owned() + "created_" + &format!("{:020}", created_file_num)))
                        .unwrap();
                }
                if ((row_num * customer_id_size) - (customer_id_prev_files_total)) >= (&customer_id_file.metadata().unwrap()).len() {
                    customer_id_prev_files_total += (&customer_id_file.metadata().unwrap()).len();
                    customer_id_file_num += 1;
                    println!("Customer ID Path: {}", &(customer_id_path.to_owned() + "customer_id_" + &format!("{:020}", customer_id_file_num)));
                    customer_id_file = OpenOptions::new()
                        .read(true)
                        .open(&(customer_id_path.to_owned() + "customer_id_" + &format!("{:020}", customer_id_file_num)))
                        .unwrap();
                }
                // Read all the next values we need
                id_file.read_exact(&mut id_buffer).unwrap();
                created_file.read_exact(&mut created_buffer).unwrap();
                customer_id_file.read_exact(&mut customer_id_buffer).unwrap();
                // Now increment customer order counts
                let customer_id = Builder::from_bytes(customer_id_buffer[0..16].try_into().unwrap()).into_uuid();
                orders_per_customer.entry(customer_id).and_modify(|counter| *counter += 1).or_insert(1);
                // Now increment month order counts
                let millis = i64::from_be_bytes(created_buffer[0..8].try_into().unwrap());
                let datetime = Utc.timestamp_millis(millis);
                let month = Utc.ymd(datetime.date_naive().year(), datetime.date_naive().month(), 1).and_hms(0,0,0);
                // println!("Created month: {}", month);
                orders_per_month.entry(month).and_modify(|counter| *counter += 1).or_insert(1);

                row_num += 1;
                order_stats.orders_count += 1;
            }
            id_prev_files_total += id_file.metadata().unwrap().len();
            id_file_num += 1;
        }
        println!("{:#?}", orders_per_month.iter().sorted());
        println!("{:#?}", order_stats);
    }

    //let id_paths = fs::read_dir(&id_path).unwrap();
    //let mut customer_count2: u64 = 0;
    //for path in id_paths {
    //    let pd = format!("{}", &path.as_ref().unwrap().path().display());
    //    customer_count2 += fs::metadata(&pd).unwrap().len()/mem::size_of::<Uuid>() as u64;
    //}
    //println!("Customers 2: {}", customer_count2);

}

