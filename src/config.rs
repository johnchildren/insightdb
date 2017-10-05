use std::fs::File;
use std::io::{self, Read};
use std::path::Path;
use serde_json::{self, Error};

#[derive(Debug, Deserialize)]
pub struct DbConfig {
    pub name: String,
    pub tables: Vec<TableConfig>,
}

impl DbConfig {
    pub fn from<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let mut s = String::new();
        file.read_to_string(&mut s)?;
        Ok(Self::from_str(&s).unwrap())
    }

    pub fn from_str(s: &str) -> Result<Self, serde_json::Error> {
        let cfg = serde_json::from_str(&s).unwrap();
        Ok(cfg)
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
    use super::*;

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
}