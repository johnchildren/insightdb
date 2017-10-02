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
            .map(|cfg| Arc::new(RwLock::new(Table::new(cfg))))
            .collect();
        Self { name, tables }
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
        let columns = config.columns.iter().map(|cfg| Column::from(cfg)).collect();
        Self {name, columns}
    }
}

pub struct Column {
    pub name: String,
    pub data: Vec<i32>,
}

impl Column {
    fn from(cfg: &ColumnConfig, n: usize) -> Self {
        let name = cfg.name.clone();
        let data = Vec::with_capacity(n);
        Self { name, data }
    }
}

/*
#[derive(Debug, PartialEq)]
pub struct InMemoryDb {
    tables: Vec<InMemoryTable>,
}

impl InMemoryDb {
    pub fn from(tables: Vec<InMemoryTable>) -> Self {
        InMemoryDb { tables: tables }
    }

    #[inline]
    pub fn get(&self, name: &str) -> Option<&InMemoryTable> {
        self.tables.iter().find(|tbl| tbl.name == name)
    }

    #[inline]
    pub fn exec(&self, cmd: &str) -> Result<InMemoryTable, &'static str> {
        let query = Query::from_str(cmd)?;
        query.exec(self)
    }
}
*/

#[cfg(test)]
mod tests {
    use super::*;
    use tables::*;
    use engine::*;
    
    fn test_table<S: Into<String>>(id: S, n: usize) -> InMemoryTable {        
        let a = InMemoryColumn::from("a", Val::IntVec(vec![1; n]));
        let b = InMemoryColumn::from("b", Val::IntVec(vec![1; n]));
        let seq: Vec<i32> = (0..n).map(|x| (x+1) as i32).collect();
        let c = InMemoryColumn::from("c", Val::IntVec(seq));
        let cols = vec![a, b, c];
        InMemoryTable::from(id.into(), cols)
    }

    fn test_db(n: usize) -> InMemoryDb {                
        let t1 = test_table("t1", n);
        let t2 = test_table("t2", n);
        let tables = vec![t1, t2];
        InMemoryDb::from(tables)
    }

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
    fn db_get() {
        let n = 10;
        let db = test_db(n);
        let tbl = test_table("t2", n);
        assert_eq!(db.get("t2").unwrap(), &tbl);
    }

    #[test]
    fn db_open() {
        let cfg = test_db_config();
        let db = Database::open(&cfg);
        assert_eq!(db.name, "db1");
        assert_eq!(db.tables.len(), 2);
        let t1 = db.tables[0];
        assert_eq!(t1.name, "t1");
        assert_eq!(t1.columns.len(), 2);
    }
}