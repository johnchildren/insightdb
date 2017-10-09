use std::io;
use std::path::Path;
use std::sync::{Arc, RwLock};
use config::*;

pub struct Database {
    pub name: String,
    pub tables: Vec<Arc<RwLock<Table>>>
}

impl Database {
    pub fn new(config: &DbConfig) -> Database {
        let name = config.name.clone();
        let tables = config.tables
            .iter()
            .map(|tbl_cfg| {
                println!("creating table: {}", tbl_cfg.name);
                Arc::new(RwLock::new(Table::new(tbl_cfg)))
            }).collect();
        Self { name, tables }
    }

    pub fn from_path<P:AsRef<Path>>(path: P) -> Result<Self, String> {
        let cfg = DbConfig::from(path)?;
        Ok(Database::new(&cfg))
    }

    pub fn start(&self) {
        println!("starting engine");
    }
}

pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn new(config: &TableConfig) -> Self {
        let name = config.name.clone();
        let n = config.size;
        let columns = config.columns.iter().map(|cfg| {
            println!("\t{}", cfg.name);
            Column::from_cfg(cfg, n)
        }).collect();
        Self {name, columns}
    }

    pub fn from<S:Into<String>>(name: S, cols: Vec<Column>) -> Self {
        Self{name: name.into(), columns: cols}
    }
}

pub struct Column {
    pub name: String,
    pub data: Vec<i32>,
}

impl Column {
    pub fn from_cfg(cfg: &ColumnConfig, n: usize) -> Self {
        let name = cfg.name.clone();
        let data = Vec::with_capacity(n);
        Self { name, data }
    }

    pub fn from(name: String, data: Vec<i32>) -> Self {
        Column{name, data}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db_config() -> DbConfig {
        DbConfig {
            name: "db1".to_string(),
            tables: vec![
                TableConfig{
                    name: "t1".to_string(),
                    size: 10,
                    columns: vec![
                        ColumnConfig{
                            name: "c1".to_string(),
                            col_type: "int".to_string(),
                        },
                        ColumnConfig{
                            name: "c2".to_string(),
                            col_type: "int".to_string(),
                        }                        
                    ],
                }, 
                TableConfig{
                    name: "t2".to_string(),
                    size: 20,
                    columns: vec![
                        ColumnConfig{
                            name: "c1".to_string(),
                            col_type: "int".to_string(),
                        },
                        ColumnConfig{
                            name: "c2".to_string(),
                            col_type: "int".to_string(),
                        }                        
                    ],
                }],
        }
    }

    #[test]
    fn db_from_path() {
        let db = Database::from_path("config.json").unwrap();
        assert_eq!(db.name, "db1");
        assert_eq!(db.tables.len(), 2);
        let n = 10;
        let t = &db.tables[0].read().unwrap(); 
        assert_eq!(t.name, "t1");
        assert_eq!(t.columns.len(), 2);
        let col = &t.columns[0];
        assert_eq!(col.name, "c1");
        assert_eq!(col.data.len(), 0);
        assert_eq!(col.data.capacity(), n);
        let col = &t.columns[1];
        assert_eq!(col.name, "c2");
        assert_eq!(col.data.len(), 0);
        assert_eq!(col.data.capacity(), n);
        let t = &db.tables[1].read().unwrap(); 
        let n = 20;
        assert_eq!(t.name, "t2");
        assert_eq!(t.columns.len(), 3);
        let col = &t.columns[0];
        assert_eq!(col.name, "c1");
        assert_eq!(col.data.len(), 0);
        assert_eq!(col.data.capacity(), n);
        let col = &t.columns[1];
        assert_eq!(col.name, "c2");
        assert_eq!(col.data.len(), 0);
        assert_eq!(col.data.capacity(), n);
        let col = &t.columns[2];
        assert_eq!(col.name, "c3");
        assert_eq!(col.data.len(), 0);
        assert_eq!(col.data.capacity(), n);        
    }

    #[test]
    fn db_open() {
        let cfg = test_db_config();
        let db = Database::new(&cfg);
        assert_eq!(db.name, "db1");
        assert_eq!(db.tables.len(), 2);
        let t1_lock = db.tables[0].read();
        let t1 = t1_lock.unwrap();
        assert_eq!(t1.name, "t1");
        assert_eq!(t1.columns.len(), 2);
        let c1 = &t1.columns[0];  
        assert_eq!(c1.name, "c1");
        let n = 10;
        assert_eq!(c1.data.len(), 0);
        assert_eq!(c1.data.capacity(), n);
        let c2 = &t1.columns[1];
        assert_eq!(c2.name, "c2");
        assert_eq!(c2.data.len(), 0);
        assert_eq!(c2.data.capacity(), n);
    }
}