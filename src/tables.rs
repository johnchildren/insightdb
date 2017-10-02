use std::cmp;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Display};
use std::io::Write;

use aggregators::{Aggregate, SumAggregator};
use engine::{Predicate, Val};
use tabwriter::TabWriter;
use rayon::prelude::ParallelSliceMut;

pub trait Table: Sized {
    fn name(&self) -> &str;

    fn get(&self, name: &str) -> Result<&InMemoryColumn, &'static str>;

    fn display(&self) -> String;
}

#[derive(Debug, PartialEq, Clone)]
pub struct InMemoryTable {
    name: String,
    cols: Vec<InMemoryColumn>,
    len: usize,
}

impl Table for InMemoryTable {
    #[inline]
    fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    fn get(&self, name: &str) -> Result<&InMemoryColumn, &'static str> {
        match self.cols.iter().find(|col| col.name == name) {
            Some(col) => Ok(col),
            None => Err("cannot find column"),
        }
    }

    fn display(&self) -> String {
        self.tostring()
    }
}


impl InMemoryTable {
    #[inline]
    pub fn from<S: Into<String>>(name: S, cols: Vec<InMemoryColumn>) -> InMemoryTable {
        let mut max_len = 0;
        for col in &cols {
            let col_len = col.len();
            if col_len > max_len {
                max_len = col_len;
            }
        }
        InMemoryTable {
            name: name.into(),
            cols: cols,
            len: max_len,
        }
    }

    pub fn tostring(&self) -> String {
        let mut s = String::new();
        for col in &self.cols {
            s.push_str(&col.name);
            s.push_str("\t");
        }
        s.push_str("\n");
        let n = cmp::min(20, self.len);
        for i in 0..n {
            for col in &self.cols {
                match col.get(i) {
                    Some(ref val) => s.push_str(val),
                    None => s.push(' '),
                }
                s.push('\t');
            }
            s.push('\n');
        }
        let mut tw = TabWriter::new(vec![]);
        tw.write_all(s.as_bytes()).unwrap();
        tw.flush().unwrap();
        String::from_utf8(tw.into_inner().unwrap()).unwrap()
    }

}

impl fmt::Display for InMemoryTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display())
    }
}

pub struct KeyedBTreeTable {
    name: String,
    keyed_cols: Vec<String>,
    cols: Vec<String>,
    data: BTreeMap<i32, SumAggregator<i32>>,
    len: usize,
}

impl KeyedBTreeTable {
    pub fn from<S: Into<String>>(
        name: S,
        keyed_cols: Vec<String>,
        cols: Vec<String>,
        data: BTreeMap<i32, SumAggregator<i32>>,
    ) -> Self {
        let n = data.len();
        KeyedBTreeTable {
            name: name.into(),
            keyed_cols: keyed_cols,
            cols: cols,
            data: data,
            len: n,
        }
    }
}

pub struct KeyedBTreeBuilder {
    pub aggs: BTreeMap<i32, SumAggregator<i32>>,
}

impl KeyedBTreeBuilder {
    pub fn new() -> Self {
        KeyedBTreeBuilder { aggs: BTreeMap::new() }
    }

    pub fn push(&mut self, key: i32, val: i32) {
        self.aggs.entry(key).or_insert(SumAggregator::new()).push(val);
    }

    pub fn build(self) -> KeyedBTreeTable {
        let key_cols = vec![String::from("c")];
        let cols = vec![String::from("sum(a)")];
        KeyedBTreeTable::from("t", key_cols, cols, self.aggs)
    }
}

pub struct KeyedTableBuilder {
    keys: Vec<i32>,
    vals: Vec<SumAggregator<i32>>,
    key_map: HashMap<i32, usize>,
}

//impl<K: Hash + Eq + Clone, V: 'static + AddAssign + Default + Clone> KeyedTableBuilder<K, V> {
impl KeyedTableBuilder {    
    pub fn new() -> Self {
        KeyedTableBuilder {
            keys: Vec::new(),
            vals: Vec::new(),
            key_map: HashMap::new(),
        }
    }

    pub fn push(&mut self, key: i32, val: i32) {
        let n = self.key_map.len();
        let i = self.key_map.entry(key.clone()).or_insert(n).clone();
        if i >= n {
            self.keys.push(key);        
            self.vals.push(SumAggregator::from(val));
        } else {
            self.vals[i].push(val);
        }
    } 

    pub fn build(mut self) -> KeyedTable {
        self.keys.par_sort();
        let key_col = InMemoryColumn::from("k", Val::IntVec(self.keys));        
        let aggs = self.vals;
        KeyedTable::from("t", key_col, aggs)
    }
}
 
pub struct KeyedTable {
    pub name: String,
    key_col: InMemoryColumn,
    aggs: Vec<SumAggregator<i32>>,
}

impl KeyedTable {
    pub fn from<S:Into<String>>(name: S, key_col: InMemoryColumn, aggs: Vec<SumAggregator<i32>>) -> Self {
        KeyedTable{ name: name.into(), key_col: key_col, aggs: aggs }
    }

    pub fn display(&self) -> String {
        let mut s = String::new();
        for keyed_col in vec!["c"] {
            s.push_str(keyed_col);
            s.push_str("\t|\t")
        }
        for col in vec!["sum(a)"] {
            s.push_str(col);
            s.push_str("\t");
        }
        s.push_str("\n");
        let n = cmp::min(10, self.len());
        let key_vec = self.key_col.int_vec().unwrap();
        for (key, agg) in key_vec.iter().zip(self.aggs.iter()).take(n) {
            s.push_str(&key.to_string());
            s.push_str("\t|\t");
            let agg_val: &i32 = agg.aggregate();
            s.push_str(&agg_val.to_string());
            s.push('\n');
        }
        let mut tw = TabWriter::new(vec![]);
        tw.write_all(s.as_bytes()).unwrap();
        tw.flush().unwrap();
        String::from_utf8(tw.into_inner().unwrap()).unwrap()        
    }

    fn len(&self) -> usize {
        self.aggs.len()
    }
}

impl Table for KeyedBTreeTable {
    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self, _: &str) -> Result<&InMemoryColumn, &'static str> {
        unimplemented!()
    }

    fn display(&self) -> String {
        let mut s = String::new();
        for keyed_col in &self.keyed_cols {
            s.push_str(&keyed_col);
            s.push_str("\t|\t")
        }
        for col in &self.cols {
            s.push_str(&col);
            s.push_str("\t");
        }
        s.push_str("\n");
        let n = cmp::min(10, self.len);
        for (key, agg) in self.data.iter().take(n) {
            s.push_str(&key.to_string());
            s.push_str("\t|\t");
            let agg_val: &i32 = agg.aggregate();
            s.push_str(&agg_val.to_string());
            s.push('\n');
        }
        let mut tw = TabWriter::new(vec![]);
        tw.write_all(s.as_bytes()).unwrap();
        tw.flush().unwrap();
        String::from_utf8(tw.into_inner().unwrap()).unwrap()
    }
}

impl Display for KeyedTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct InMemoryColumn {
    pub name: String,
    pub val: Val,
}

impl InMemoryColumn {
    #[inline]
    pub fn from<S: Into<String>>(name: S, val: Val) -> Self {
        InMemoryColumn {
            name: name.into(),
            val: val,
        }
    }

    #[inline]
    pub fn get(&self, pos: usize) -> Option<String> {
        self.val.get(pos)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.val.len()
    }

    #[inline]
    pub fn add(&self, rhs: &InMemoryColumn) -> Result<InMemoryColumn, ()> {
        let name = self.name.clone() + " + " + &rhs.name;
        let val = self.val.add(&rhs.val)?;
        Ok(InMemoryColumn::from(name, val))
    }

    #[inline]
    pub fn sub(&self, rhs: &InMemoryColumn) -> Result<InMemoryColumn, ()> {
        let name = self.name.clone() + " - " + &rhs.name;
        let val = self.val.sub(&rhs.val)?;
        Ok(InMemoryColumn::from(name, val))
    }

    #[inline]
    pub fn mul(&self, rhs: &InMemoryColumn) -> Result<InMemoryColumn, ()> {
        let name = self.name.clone() + " * " + &rhs.name;
        let val = self.val.mul(&rhs.val)?;
        Ok(InMemoryColumn::from(name, val))
    }

    #[inline]
    pub fn div(&self, rhs: &InMemoryColumn) -> Result<InMemoryColumn, ()> {
        let name = self.name.clone() + " / " + &rhs.name;
        let val = self.val.div(&rhs.val)?;
        Ok(InMemoryColumn::from(name, val))
    }

    #[inline]
    pub fn sum(&self) -> Result<InMemoryColumn, &'static str> {
        let name = "sum(".to_string() + &self.name + ")";
        let val = self.val.sum()?;
        Ok(InMemoryColumn::from(name, val))
    }

    #[inline]
    pub fn sums(&self) -> Result<InMemoryColumn, &'static str> {
        let name = "sums(".to_string() + &self.name + ")";
        let val = self.val.sums()?;
        Ok(InMemoryColumn::from(name, val))
    }

    #[inline]
    pub fn min(&self) -> Result<InMemoryColumn, &'static str> {
        let name = "min(".to_string() + &self.name + ")";
        let val = self.val.min()?;
        Ok(InMemoryColumn::from(name, val))
    }

    #[inline]
    pub fn mins(&self) -> Result<InMemoryColumn, &'static str> {
        let name = "min(".to_string() + &self.name + ")";
        let val = self.val.mins()?;
        Ok(InMemoryColumn::from(name, val))
    }

    #[inline]
    pub fn max(&self) -> Result<InMemoryColumn, &'static str> {
        let name = "max(".to_string() + &self.name + ")";
        let val = self.val.max()?;
        Ok(InMemoryColumn::from(name, val))
    }

    #[inline]
    pub fn maxs(&self) -> Result<InMemoryColumn, &'static str> {
        let name = "max(".to_string() + &self.name + ")";
        let val = self.val.maxs()?;
        Ok(InMemoryColumn::from(name, val))
    }

    #[inline]
    pub fn product(&self) -> Result<InMemoryColumn, &'static str> {
        let name = "product(".to_string() + &self.name + ")";
        let val = self.val.product()?;
        Ok(InMemoryColumn::from(name, val))
    }

    #[inline]
    pub fn products(&self) -> Result<InMemoryColumn, &'static str> {
        let name = "product(".to_string() + &self.name + ")";
        let val = self.val.products()?;
        Ok(InMemoryColumn::from(name, val))
    }

    #[inline]
    pub fn range(&self) -> Result<InMemoryColumn, &'static str> {
        let name = "range(".to_string() + &self.name + ")";
        let val = self.val.range()?;
        Ok(InMemoryColumn::from(name, val))
    }

    #[inline]
    pub fn unique(&self) -> Result<InMemoryColumn, &'static str> {
        let name = "unique(".to_string() + &self.name + ")";
        let val = self.val.unique()?;
        Ok(InMemoryColumn::from(name, val))
    }    

    #[inline]
    pub fn filter_gate(&self, pred: Predicate, val: Val) -> Vec<usize> {
        self.val.filter_gate(pred, val)
    }

    #[inline]
    pub fn int_vec(&self) -> Result<&[i32], &'static str> {
        match self.val {
            Val::IntVec(ref vec) => Ok(vec),
            _ => Err("expected int vec"),
        } 
    }
    
}

/*
#[cfg(test)]
mod tests {
    use super::*;

}
*/