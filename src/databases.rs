use tables::{Table, InMemoryTable, KeyedInMemoryTable};
use engine::Query;

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
        self.tables.iter().find(|tbl| tbl.name() == name)
    }

    #[inline]
    pub fn exec(&self, cmd: &str) -> Result<Box<Table>, &'static str> {
        let query = Query::from_str(cmd)?;
        if query.has_groupings() {
            return query.exec_keyed(self)
        }
        query.exec(self)
    }

    #[inline]
    pub fn exec_keyed(&self, cmd: &str) -> Result<Box<Table>, &'static str> {
        let query = Query::from_str(cmd)?;
        query.exec_keyed(self)
    }    
}

#[cfg(test)]
mod tests {
    use super::*;
    use tables::*;
    use engine::*;
    use test::Bencher;
    
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

    #[test]
    fn db_get() {
        let n = 10;
        let db = test_db(n);
        let tbl = test_table("t2", n);
        assert_eq!(db.get("t2").unwrap(), &tbl);
    }
}