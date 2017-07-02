use std::collections::BTreeMap;

pub struct KeyedTable {
    table: BTreeMap<i32, Vec<i32>
}

impl KeyedTable {
    fn new() -> Self {
        KeyedTable {
            table: BTreeMap::new(),
        }
    }

    fn from(table: BTreeMap<i32, Vec<i32>>) -> Self {
        KeyedTable {
            table: table,
        }
    }
}

