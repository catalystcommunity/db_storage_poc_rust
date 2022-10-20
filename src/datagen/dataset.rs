use super::constants::{DATA_DIRECTORY,FILE_SIZE};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::fs;


// This would eventually be a full header, and it might just be a pointer to a metadata file set
pub struct TableMetaData {
    pub table_name: String,
    pub columns: u16,
    pub rows: u64,
}

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
        
        return Ok(ret_val);
    }

    pub fn insert_data(table: Table) -> Result<String, String> {
        fs::create_dir_all(DATA_DIRECTORY).unwrap();
        for (col, vals) in table.data {
            fs::create_dir_all(format!("{}",DATA_DIRECTORY.to_owned()+"/"+&col)).unwrap();
            // First, see what files are in there for the table/column
            // Do this on a per COLUMN basis, not table
            // See what room there is
            // If there's room, add to it the rows you can
            // If there isn't, or when there isn't, create the next numbered file
            // Repeat until every column is done
            let mut highest:u64 = 0;
            let paths = fs::read_dir(format!("{}",DATA_DIRECTORY.to_owned()+"/"+&col)).unwrap();
            for path in paths {
                let pd = format!("{}", path.unwrap().path().display());
                let (_, num) = pd.rsplit_once("_").unwrap();
                let current_num = num.parse::<u64>().unwrap();
                if current_num > highest {
                    highest = current_num;
                }
            }
        }
        return Ok("".to_string());
    }
}
