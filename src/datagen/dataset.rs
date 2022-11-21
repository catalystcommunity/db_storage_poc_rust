use super::constants::{DATA_DIRECTORY,FILE_SIZE};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::{collections::HashMap, fs::File};
use std::fs;
use std::fs::{DirEntry,OpenOptions};
use std::io::Write;
use std::mem;
use std::ops::Range;

// This would eventually be a full header, and it might just be a pointer to a metadata file set
pub struct TableMetaData {
    pub table_name: String,
    pub columns: u16,
    pub rows: u64,
}

// #[derive(Debug)]
//pub enum ColumnSlice<'a> {
//    String(&'a [String]),
//    Int64(&'a [i64]),
//    Int8(&'a [i8]),
//    UInt64(&'a [u64]),
//    UInt8(&'a [u8]),
//    DateTime(&'a [DateTime<Utc>]),
//    Decimal(&'a [Decimal]),
//    Uuid(&'a [Uuid]),
//    ForeignKey(&'a [Uuid]),
//}

pub enum Column {
    String(Vec<String>),
    Int64(Vec<i64>),
    Int8(Vec<i8>),
    UInt64(Vec<u64>),
    UInt8(Vec<u8>),
    DateTime(Vec<DateTime<Utc>>),
    Decimal(Vec<Decimal>),
    Uuid(Vec<Uuid>),
    ForeignKey(Vec<Uuid>),
}

impl Column {
    pub fn len(&self) -> usize {
        match self {
            Column::String(val) => val.len(),
            Column::Int64(val) => val.len(),
            Column::Int8(val)  => val.len(),
            Column::UInt64(val) => val.len(),
            Column::UInt8(val) => val.len(),
            Column::DateTime(val) => val.len(),
            Column::Decimal(val) => val.len(),
            Column::Uuid(val) => val.len(),
            Column::ForeignKey(val) => val.len(),
        }
    }

    pub fn write_data(&self, directory: &String, column_name: &String) -> Result<u64,String>
    {
        // Write data, return the latest filename to be using
        let each_size = match self {
            Column::String(_) => mem::size_of::<u8>()*20,
            Column::Int64(_) => mem::size_of::<i64>(),
            Column::Int8(_)  => mem::size_of::<i8>(),
            Column::UInt64(_) => mem::size_of::<u64>(),
            Column::UInt8(_) => mem::size_of::<u8>(),
            // DateTime will get stored as an i64 from millis to bytes
            Column::DateTime(_) => mem::size_of::<i64>(),
            Column::Decimal(_) => mem::size_of::<Decimal>(),
            Column::Uuid(_) => mem::size_of::<Uuid>(),
            Column::ForeignKey(_) => mem::size_of::<Uuid>(),
        };
        let mut records_written:usize = 0;

        // First, see what files are in there for the table/column
        // Do this on a per COLUMN basis, not table
        // See what room there is
        // If there's room, add to it the rows you can
        // If there isn't, or when there isn't, create the next numbered file
        // Repeat until every column is done
        let mut highest:u64 = 0;
        let mut last_file:String = "".to_string();
        let paths = fs::read_dir(directory).unwrap();
        for path in paths {
            let pd = format!("{}", &path.as_ref().unwrap().path().display());
            let (_, num) = pd.rsplit_once('_').unwrap();
            let current_num = num.parse::<u64>().unwrap();
            if current_num > highest {
                highest = current_num;
                last_file = path.unwrap().file_name().to_str().unwrap().to_string();
            }
        }
        if !last_file.is_empty() {
            println!("Last File: {}", directory.to_owned() + &last_file);
        }
        if !last_file.is_empty() && fs::metadata(directory.to_owned() + &last_file).unwrap().len() >= FILE_SIZE  {
            // Technically we'd want to worry about why it's greater than the file size
            // limit, but alas, here we are in PoC land
            highest += 1;
        }

        while records_written < self.len() {
            let num_part = format!("{:020}", highest);
            let file_name = format!("{}", column_name.to_owned() + "_" + &num_part); 
            let full_path = format!("{}",directory.to_owned()+&file_name);
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(&full_path)
                .unwrap();
            let current_file_size = fs::metadata(&full_path).unwrap().len();
            let mut size_left = 0;
            if current_file_size <= FILE_SIZE {
                size_left = FILE_SIZE - current_file_size;
            }
            let records_available = size_left as usize / each_size;
            //println!("{}", "size_left: ".to_owned() + &size_left.to_string() + " records_available: " + &records_available.to_string());
            if records_available == 0 || size_left == 0 {
                highest += 1;
                continue;
            }
 
            match self {
                Column::String(val) => {
                    let mut limit = val.len();
                    if limit > records_available + records_written {
                        limit = records_available + records_written;
                    }
                    let values = &val[records_written..limit];
                    for item in values {
                        let _ = writeln!(file, "{}", &item);
                    }
                    records_written = limit;
                },
                Column::Int8(val) => {
                    let mut limit = val.len();
                    if limit > records_available + records_written {
                        limit = records_available + records_written;
                    }
                    let values = &val[records_written..limit];
                    for item in values {
                        let _ = file.write_all(&item.to_be_bytes());
                    }
                    records_written = limit;
                },
                Column::Int64(val) => {
                    let mut limit = val.len();
                    if limit > records_available + records_written {
                        limit = records_available + records_written;
                    }
                    let values = &val[records_written..limit];
                    for item in values {
                        let _ = file.write_all(&item.to_be_bytes());
                    }
                    records_written = limit;
                },
                Column::UInt8(val) => {
                    let mut limit = val.len();
                    if limit > records_available + records_written {
                        limit = records_available + records_written;
                    }
                    let values = &val[records_written..limit];
                    for item in values {
                        let _ = file.write_all(&item.to_be_bytes());
                    }
                    records_written = limit;
                },
                Column::UInt64(val) => {
                    let mut limit = val.len();
                    if limit > records_available + records_written {
                        limit = records_available + records_written;
                    }
                    let values = &val[records_written..limit];
                    for item in values {
                        let _ = file.write_all(&item.to_be_bytes());
                    }
                    records_written = limit;
                },
                Column::DateTime(val) => {
                    let mut limit = val.len();
                    if limit > records_available + records_written {
                        limit = records_available + records_written;
                    }
                    let values = &val[records_written..limit];
                    for item in values {
                        let _ = file.write_all(&item.timestamp_millis().to_be_bytes());
                    }
                    records_written = limit;
                },
                Column::Decimal(val) => {
                    let mut limit = val.len();
                    if limit > records_available + records_written {
                        limit = records_available + records_written;
                    }
                    let values = &val[records_written..limit];
                    for item in values {
                        let _ = file.write_all(&item.serialize());
                    }
                    records_written = limit;
                },
                Column::Uuid(val) => {
                    let mut limit = val.len();
                    if limit > records_available + records_written {
                        limit = records_available + records_written;
                    }
                    let values = &val[records_written..limit];
                    for item in values {
                        let _ = file.write_all(item.as_bytes());
                    }
                    records_written = limit;
                },
                Column::ForeignKey(val) => {
                    let mut limit = val.len();
                    if limit > records_available + records_written {
                        limit = records_available + records_written;
                    }
                    let values = &val[records_written..limit];
                    for item in values {
                        let _ = file.write_all(item.as_bytes());
                    }
                    records_written = limit;
                },
            };
        }
        Ok(highest)
    }
}


pub struct Table {
    pub meta: TableMetaData,
    pub id_column: String,
    pub data: HashMap<String, Column>,
}

impl Table {
    pub fn new(id_column: String, meta: TableMetaData, data: HashMap<String, Column>) -> Result<Table,String> {
        if !data.contains_key(&id_column) {
            let error = Err(format!("Column with name {:?} not in data", id_column));
            return error;
        }
        let ret_val: Table = Table{
            id_column: id_column,
            meta: meta,
            data: data,
        };
        
        Ok(ret_val)
    }

    pub fn write_data(&self) -> Result<String, String> {
        let mut column_latest_files:HashMap<String, u64> = HashMap::new();
        let table_dir = DATA_DIRECTORY.to_owned()+&self.meta.table_name;
        fs::create_dir_all(&table_dir).unwrap();
        for (col_name, data) in &self.data {
            let col_dir = format!("{}",table_dir.to_owned()+"/"+&col_name+"/");
            fs::create_dir_all(&col_dir).unwrap();
            let highest_filenum = data.write_data(&col_dir, &col_name).unwrap();
            column_latest_files.insert(col_name.to_string(), highest_filenum);
        }
        Ok("".to_string())
    }
}

//fn to_date_time_string(datetime_bytes: &[u8]) -> DateTime<Utc>
//{
//    let millis = i64::from_le_bytes(datetime_bytes.try_into().unwrap());
//    Utc.timestamp_millis(millis)
//}




