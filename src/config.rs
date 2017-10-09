use csv::Reader;
use std::fs::File;
use std::io::{Read};
use std::path::Path;
use serde_json::{self};
use database::{Column, Table};

fn read_csv<S:Into<String>, P:AsRef<Path>>(name: S, path: P) -> Result<Table, String> {
    let mut r = match Reader::from_file(path) {
        Ok(r) => r,
        Err(_) => return Err(String::from("cannot open csv file")),
    };
    let headers = match r.headers() {
        Ok(h) => h,
        Err(_) => return Err(String::from("cannot get headers")),
    };
    let mut data: Vec<Vec<i32>> = headers.iter().map(|_| Vec::new()).collect();
    for record in r.records() {
        let row = record.unwrap();
        for (col, vec) in row.iter().zip(data.iter_mut()) {
            let val: i32 = match col.parse() {
                Ok(val) => val,
                Err(_) => return Err(String::from("cannot parse col")),
            };
            vec.push(val);
        }
    }
    let mut columns: Vec<Column> = Vec::with_capacity(headers.len());
    for (name, data) in headers.into_iter().zip(data.into_iter()) {
        columns.push(Column::from(name, data));
    }
    Ok(Table::from(name.into(), columns))
}

#[derive(Debug, Deserialize)]
pub struct DbConfig {
    pub name: String,
    pub tables: Vec<TableConfig>,
}

impl DbConfig {
    pub fn from<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(_) => return Err(String::from("cannot open config  file")),
        };
        let mut s = String::new();
        if let Err(_) = file.read_to_string(&mut s) {
            return Err(String::from("cannot read config file"));
        }
        Self::from_str(&s)
    }

    pub fn from_str(s: &str) -> Result<Self, String> {
        serde_json::from_str(&s).map_err(|_| String::from("cannot parse config json"))
    }
}

#[derive(Debug, Deserialize)]
pub struct TableConfig {
    pub name: String,
    pub columns: Vec<ColumnConfig>,
    pub size: usize,
}

#[derive(Debug, Deserialize)]
pub struct ColumnConfig {
    pub name: String,
    pub col_type: String,
}

#[cfg(test)]
mod tests {
    use csv::Writer;
    use super::*;

    fn write_test_csv<P:AsRef<Path>>(path: P)  {
        let mut w = Writer::from_file(path).unwrap();
        w.encode(&["a", "b", "c"]).unwrap();
        for i in 0..10 {
            let val = i as i32;
            w.encode(&[i, i, i]).unwrap();
        }
        let _ = w.flush().unwrap();
    }

    #[test]
    fn dbconfig_from() {
        let s = r#"{
            "name": "db1",
            "tables": [
                {
                    "name": "t1",
                    "columns": [
                        {
                            "name": "c1",
                            "col_type": "int"
                        },
                        {
                            "name": "c2",
                            "col_type": "int"
                        }                        
                    ],
                    "size": 10
                },
                {
                    "name": "t2",
                    "columns": [
                        {
                            "name": "c1",
                            "col_type": "int"
                        },
                        {
                            "name": "c2",
                            "col_type": "int"
                        },    
                        {
                            "name": "c3",
                            "col_type": "int"
                        }                                         
                    ],
                    "size": 10
                }                  
            ]
        }"#;
        let cfg = DbConfig::from_str(&s).unwrap();
        assert_eq!(cfg.name, "db1");
        assert_eq!(cfg.tables.len(), 2);
        let t1 = &cfg.tables[0];
        assert_eq!(t1.name, "t1");
        assert_eq!(t1.columns.len(), 2);
        assert_eq!(t1.size, 10);
        let c1 = &t1.columns[0];
        assert_eq!(c1.name, "c1");
        let c2 = &t1.columns[1];
        assert_eq!(c2.name, "c2");
        let t2 = &cfg.tables[1];
        assert_eq!(t2.name, "t2");
        assert_eq!(t2.columns.len(), 3);
        assert_eq!(t2.size, 10);
        let c1 = &t2.columns[0];
        assert_eq!(c1.name, "c1");
        let c2 = &t2.columns[1];
        assert_eq!(c2.name, "c2");  
        let c3 = &t2.columns[2];
        assert_eq!(c3.name, "c3");      
    }

    #[test]
    fn read_csv_ok() {
        let path = "test.csv";
        write_test_csv(path);
        let tbl = read_csv("t", path).unwrap();
        assert_eq!(tbl.name, "t");
        assert_eq!(tbl.columns.len(), 3);
        assert_eq!(tbl.columns[0].name, "a");
        assert_eq!(tbl.columns[1].name, "b");
        assert_eq!(tbl.columns[2].name, "c");
        let n = 10;
        assert_eq!(tbl.columns[0].data.len(), n);
        assert_eq!(tbl.columns[1].data.len(), n);
        assert_eq!(tbl.columns[2].data.len(), n);
        for i in 0..n {
            let val = i as i32;
            for col in &tbl.columns {
                assert_eq!(col.data[i], val);
            }
        }
    }
}