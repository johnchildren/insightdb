use std::cmp;
use std::fmt::{self, Debug, Display};
use std::io::Write;

use engine::{Predicate, Filter, Val};
use tabwriter::TabWriter;

pub trait Table: Display {
    fn name(&self) -> &str;

    fn get(&self, name: &str) -> Option<&InMemoryColumn>;
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
    fn get(&self, name: &str) -> Option<&InMemoryColumn> {
        self.cols.iter().find(|col| col.name == name)
    }
}

impl InMemoryTable {
    #[inline]
    pub fn from<S: Into<String>>(name: S, cols: Vec<InMemoryColumn>) -> Self {
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

    #[inline]
    pub fn get_2(&self, t1: &str, t2: &str) -> Option<(&InMemoryColumn, &InMemoryColumn)> {
        let col1 = match self.get(t1) {
            Some(col) => col,
            None => return None,
        };
        let col2 = match self.get(t2) {
            Some(col) => col,
            None => return None,
        };
        Some((col1, col2))
    }
}

impl fmt::Display for InMemoryTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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
        let written = String::from_utf8(tw.into_inner().unwrap()).unwrap();
        write!(f, "{}", written)
    }
}

pub struct KeyedInMemoryTable {
    name: String,
    keyed_cols: Vec<InMemoryColumn>,
    cols: Vec<InMemoryColumn>,
    len: usize,
}

impl KeyedInMemoryTable {
    pub fn from<S: Into<String>>(name: S, keyed_cols: Vec<InMemoryColumn>, cols: Vec<InMemoryColumn>) -> Self {
        let n = keyed_cols[0].len();
        KeyedInMemoryTable {
            name: name.into(),
            keyed_cols: keyed_cols,
            cols: cols,
            len: n,
        }
    }
}

impl Table for KeyedInMemoryTable {
    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self, name: &str) -> Option<&InMemoryColumn> {
        self.keyed_cols.iter().chain(self.cols.iter()).find(|col| {
            col.name == name
        })
    }
}

impl Display for KeyedInMemoryTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut s = String::new();
        for keyed_col in &self.keyed_cols {
            s.push_str(&keyed_col.name);
            s.push_str("\t|\t")
        }
        for col in &self.cols {
            s.push_str(&col.name);
            s.push_str("\t");
        }
        s.push_str("\n");
        let n = cmp::min(20, self.len);
        for i in 0..n {
            for keyed_col in &self.keyed_cols {
                match keyed_col.get(i) {
                    Some(ref val) => s.push_str(val),
                    None => s.push(' '),
                }
            }
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
        let written = String::from_utf8(tw.into_inner().unwrap()).unwrap();
        write!(f, "{}", written)
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
    pub fn filter_gate(&self, pred: Predicate, val: Val) -> Vec<usize> {
        self.val.filter_gate(pred, val)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;

    fn test_table<S: Into<String>>(id: S, n: usize) -> InMemoryTable {        
        let a = InMemoryColumn::from("a", Val::IntVec(vec![1; n]));
        let b = InMemoryColumn::from("b", Val::IntVec(vec![1; n]));
        let seq: Vec<i32> = (0..n).map(|x| (x+1) as i32).collect();
        let c = InMemoryColumn::from("c", Val::IntVec(seq));
        let cols = vec![a, b, c];
        InMemoryTable::from(id.into(), cols)
    }
}