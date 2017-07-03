use std::cmp;
use std::fmt;
use std::io::Write;
use std::time::Instant;

use chrono::prelude::*;
use parser::Parser;
use tabwriter::TabWriter;

use computation::*;

#[derive(Clone, Debug, PartialEq)]
pub enum Predicate {
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    Equal,
}

#[derive(Debug, PartialEq)]
pub struct Filter {
    lhs: Expr,
    op: Predicate,
    rhs: Expr,
}

impl Filter {
    pub fn new(lhs: Expr, op: Predicate, rhs: Expr) -> Self {
        Filter {
            lhs: lhs,
            op: op,
            rhs: rhs,
        }
    }

    fn apply(&self, table: &Table) -> Result<Vec<usize>, &'static str> {
        match (&self.lhs, &self.op, &self.rhs) {
            (&Expr::Id(ref id), op, &Expr::Int(val)) => {
                let col = match table.get(id) {
                    Some(col) => col,
                    None => return Err("cannot find col"),
                };
                let gate = col.filter_gate(op.clone(), Val::Int(val));
                Ok(gate)
            }
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Expr {
    Id(String),
    Int(i32),
    UnrFn(UnrOp, Box<Expr>),
    BinFn(Box<Expr>, BinOp, Box<Expr>),
    Str(String),
    DateTime(DateTime<Utc>),
}

impl Expr {
    #[inline]
    fn eval<'b>(&self, tbl: &'b Table) -> Result<Column, &'static str> {
        match *self {
            Expr::Id(ref id) => {
                match tbl.get(id) {
                    Some(col) => Ok(col.clone()),
                    None => Err("cannot find col"),
                }
            }
            Expr::Str(ref s) => {
                let name = s.clone();
                let val = Val::Str(s.clone());
                Ok(Column::from(name, val))
            }
            Expr::Int(val) => Ok(Column::from(val.to_string(), Val::Int(val))),
            Expr::UnrFn(UnrOp::Til, box Expr::Int(n)) => {
                let name = "til(".to_string() + &n.to_string() + ")";
                let val = Val::IntVec(ranged_vec(0, n as usize));
                Ok(Column::from(name, val))
            }
            Expr::UnrFn(UnrOp::Sum, box Expr::UnrFn(UnrOp::Til, box Expr::Int(n))) => {
                println!("inside");
                let name = "sum(til(".to_string() + &n.to_string() + "))";
                let val = Val::Int(n * (n + 1) / 2);
                Ok(Column::from(name, val))
            }            
            Expr::UnrFn(UnrOp::Sum, box Expr::UnrFn(UnrOp::Range, box Expr::Int(n))) => {
                let name = "sum(range(".to_string() + &n.to_string() + "))";
                let val = Val::Int(n * (n + 1) / 2);
                Ok(Column::from(name, val))
            }
            Expr::UnrFn(UnrOp::Sum, box Expr::Id(ref id)) => {
                let col = match tbl.get(id) {
                    Some(col) => col,
                    None => return Err("cannot find col"),
                };
                return col.sum();
            }
            Expr::UnrFn(UnrOp::Sums, box Expr::Id(ref id)) => {
                let col = match tbl.get(id) {
                    Some(col) => col,
                    None => return Err("cannot find col"),
                };
                return col.sums();
            }            
            Expr::UnrFn(UnrOp::Min, box Expr::Id(ref id)) => {
                let col = match tbl.get(id) {
                    Some(col) => col,
                    None => return Err("cannot find col"),
                };
                return col.min();
            }   
            Expr::UnrFn(UnrOp::Mins, box Expr::Id(ref id)) => {
                let col = match tbl.get(id) {
                    Some(col) => col,
                    None => return Err("cannot find col"),
                };
                return col.mins();
            }               
            Expr::UnrFn(UnrOp::Max, box Expr::Id(ref id)) => {
                let col = match tbl.get(id) {
                    Some(col) => col,
                    None => return Err("cannot find col"),
                };
                return col.max();
            } 
            Expr::UnrFn(UnrOp::Maxs, box Expr::Id(ref id)) => {
                let col = match tbl.get(id) {
                    Some(col) => col,
                    None => return Err("cannot find col"),
                };
                return col.maxs();
            }             
            Expr::UnrFn(UnrOp::Product, box Expr::Id(ref id)) => {
                let col = match tbl.get(id) {
                    Some(col) => col,
                    None => return Err("cannot find col"),
                };
                return col.product();
            }  
            Expr::UnrFn(UnrOp::Products, box Expr::Id(ref id)) => {
                let col = match tbl.get(id) {
                    Some(col) => col,
                    None => return Err("cannot find col"),
                };
                return col.products();
            }    
            Expr::UnrFn(UnrOp::Range, box Expr::Int(n)) => {
                let val = Val::IntVec(ranged_vec(0, n as usize));
                let name = "range(".to_string() + &n.to_string() + ")";
                Ok(Column::from(name, val))
            } 
            Expr::UnrFn(UnrOp::Range, box Expr::Id(ref id)) => {
                let col = tbl.get(id).unwrap();
                col.range()
            }                                                                        
            Expr::BinFn(box Expr::Int(lhs), BinOp::Range, box Expr::Int(rhs)) => {
                let name = "range(".to_string() + &lhs.to_string() + ", " + &rhs.to_string() + ")";
                let val = Val::IntVec(ranged_vec(lhs as usize, rhs as usize));
                return Ok(Column::from(name, val));
            }
            Expr::BinFn(box Expr::Str(ref lhs), BinOp::Add, box Expr::Str(ref rhs)) => {
                let val = lhs.to_string() + &rhs;
                let name = val.clone();
                return Ok(Column::from(name, Val::Str(val)));
            }
            Expr::BinFn(box Expr::Id(ref id1), BinOp::Add, box Expr::Id(ref id2)) => {
                let (lhs, rhs) = match tbl.get_2(id1, id2) {
                    Some(cols) => cols,
                    None => return Err("cannot find cols"),
                };
                lhs.add(rhs).map_err(|_| "cannot evaluate col add")
            }
            Expr::BinFn(ref lhs, BinOp::Add, ref rhs) => {
                let lcol = lhs.eval(tbl)?;
                let rcol = rhs.eval(tbl)?;
                return lcol.add(&rcol).map_err(|_| "cannot add exprs");
            }   
            Expr::BinFn(box Expr::Id(ref id1), BinOp::Sub, box Expr::Id(ref id2)) => {
                let (lhs, rhs) = match tbl.get_2(id1, id2) {
                    Some(cols) => cols,
                    None => return Err("cannot find cols"),
                };
                lhs.sub(rhs).map_err(|_| "cannot evaluate col sub")
            }              
            Expr::BinFn(ref lhs, BinOp::Sub, ref rhs) => {
                let lcol = lhs.eval(tbl)?;
                let rcol = rhs.eval(tbl)?;
                return lcol.sub(&rcol).map_err(|_| "cannot sub exprs");
            }    
            Expr::BinFn(box Expr::Id(ref id1), BinOp::Mul, box Expr::Id(ref id2)) => {
                let (lhs, rhs) = match tbl.get_2(id1, id2) {
                    Some(cols) => cols,
                    None => return Err("cannot find cols"),
                };
                lhs.mul(rhs).map_err(|_| "cannot evaluate col sub")
            }                 
            Expr::BinFn(ref lhs, BinOp::Mul, ref rhs) => {
                let lcol = lhs.eval(tbl)?;
                let rcol = rhs.eval(tbl)?;
                return lcol.mul(&rcol).map_err(|_| "cannot mul exprs");
            }                                    
            Expr::BinFn(box Expr::Id(ref id1), BinOp::Div, box Expr::Id(ref id2)) => {
                let (lhs, rhs) = match tbl.get_2(id1, id2) {
                    Some(cols) => cols,
                    None => return Err("cannot find cols"),
                };
                lhs.div(rhs).map_err(|_| "cannot evaluate col sub")
            }                                    
            ref expr => unimplemented!("expr={:?}", expr),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Range,
}

#[derive(Debug, PartialEq)]
pub enum UnrOp {
    Sum,
    Sums,
    Max,
    Maxs,
    Min,
    Mins,
    Avg,
    Product,
    Products,
    Range,
    Til,
}

#[derive(Debug, PartialEq)]
pub struct Query {
    select: Vec<Expr>,
    by: Option<Vec<Expr>>,
    from: Expr,
    filters: Option<Vec<Filter>>,
}

impl Query {
    pub fn from(
        select: Vec<Expr>,
        by: Option<Vec<Expr>>,
        from: Expr,
        filters: Option<Vec<Filter>>,
    ) -> Self {
        Query {
            select: select,
            by: by,
            from: from,
            filters: filters,
        }
    }

    pub fn from_str(cmd: &str) -> Result<Self, &'static str> {
        let mut parser = Parser::new(cmd);
        parser.parse()
    }

    #[inline]
    pub fn exec(&self, db: &Database) -> Result<Table, &'static str> {
        let tbl_id = match self.from {
            Expr::Id(ref id) => id,
            _ => return Err("unexpteced from expr"),
        };
        let tbl = match db.get(tbl_id) {
            Some(tbl) => tbl,
            None => return Err("cannot find table"),
        };
        let mut cols = Vec::new();
        let start = Instant::now();
        for expr in &self.select {
            match expr.eval(tbl) {
                Ok(col) => cols.push(col),
                Err(err) => return Err(err),
            }
        }
        println!("{:?}", start.elapsed());
        Ok(Table::from(tbl_id.to_string(), cols))
    }
}

#[derive(Debug, PartialEq)]
pub struct Database {
    tables: Vec<Table>,
}

impl Database {
    pub fn from(tables: Vec<Table>) -> Self {
        Database { tables: tables }
    }

    #[inline]
    fn get(&self, name: &str) -> Option<&Table> {
        self.tables.iter().find(|tbl| tbl.name == name)
    }

    #[inline]
    pub fn exec(&self, cmd: &str) -> Result<Table, &'static str> {
        let query = Query::from_str(cmd)?;
        query.exec(self)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Table {
    name: String,
    cols: Vec<Column>,
    len: usize,
}

impl Table {
    pub fn from<S: Into<String>>(name: S, cols: Vec<Column>) -> Self {
        let mut max_len = 0;
        for col in &cols {
            let col_len = col.len();
            if col_len > max_len {
                max_len = col_len;
            }
        }
        Table {
            name: name.into(),
            cols: cols,
            len: max_len,
        }
    }

    pub fn get(&self, name: &str) -> Option<&Column> {
        self.cols.iter().find(|col| col.name == name)
    }

    pub fn get_2(&self, t1: &str, t2: &str) -> Option<(&Column, &Column)> {
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

impl fmt::Display for Table {
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

#[derive(Debug, PartialEq, Clone)]
pub struct Column {
    name: String,
    val: Val,
}

impl Column {
    pub fn from<S: Into<String>>(name: S, val: Val) -> Self {
        Column {
            name: name.into(),
            val: val,
        }
    }

    #[inline]
    fn get(&self, pos: usize) -> Option<String> {
        self.val.get(pos)
    }

    #[inline]
    fn len(&self) -> usize {
        self.val.len()
    }

    pub fn add(&self, rhs: &Column) -> Result<Column, ()> {
        let name = self.name.clone() + " + " + &rhs.name;
        let val = self.val.add(&rhs.val)?;
        Ok(Column::from(name, val))
    }

    pub fn sub(&self, rhs: &Column) -> Result<Column, ()> {
        let name = self.name.clone() + " - " + &rhs.name;
        let val = self.val.sub(&rhs.val)?;
        Ok(Column::from(name, val))
    }

    pub fn mul(&self, rhs: &Column) -> Result<Column, ()> {
        let name = self.name.clone() + " * " + &rhs.name;
        let val = self.val.mul(&rhs.val)?;
        Ok(Column::from(name, val))
    }

    pub fn div(&self, rhs: &Column) -> Result<Column, ()> {
        let name = self.name.clone() + " / " + &rhs.name;
        let val = self.val.div(&rhs.val)?;
        Ok(Column::from(name, val))
    }

    #[inline]
    fn sum(&self) -> Result<Column, &'static str> {
        let name = "sum(".to_string() + &self.name + ")";
        let val = self.val.sum()?;
        Ok(Column::from(name, val))
    }

    fn sums(&self) -> Result<Column, &'static str> {
        let name = "sums(".to_string() + &self.name + ")";
        let val = self.val.sums()?;
        Ok(Column::from(name, val))
    }

    fn min(&self) -> Result<Column, &'static str> {
        let name = "min(".to_string() + &self.name + ")";
        let val = self.val.min()?;
        Ok(Column::from(name, val))
    }

    fn mins(&self) -> Result<Column, &'static str> {
        let name = "min(".to_string() + &self.name + ")";
        let val = self.val.mins()?;
        Ok(Column::from(name, val))
    }

    fn max(&self) -> Result<Column, &'static str> {
        let name = "max(".to_string() + &self.name + ")";
        let val = self.val.max()?;
        Ok(Column::from(name, val))
    }

    fn maxs(&self) -> Result<Column, &'static str> {
        let name = "max(".to_string() + &self.name + ")";
        let val = self.val.maxs()?;
        Ok(Column::from(name, val))
    }

    fn product(&self) -> Result<Column, &'static str> {
        let name = "product(".to_string() + &self.name + ")";
        let val = self.val.product()?;
        Ok(Column::from(name, val))
    }

    fn products(&self) -> Result<Column, &'static str> {
        let name = "product(".to_string() + &self.name + ")";
        let val = self.val.products()?;
        Ok(Column::from(name, val))
    }

    fn range(&self) -> Result<Column, &'static str> {
        let name = "range(".to_string() + &self.name + ")";
        let val = self.val.range()?;
        Ok(Column::from(name, val))
    }

    fn filter_gate(&self, pred: Predicate, val: Val) -> Vec<usize> {
        self.val.filter_gate(pred, val)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Val {
    IntVec(Vec<i32>),
    Int(i32),
    StrVec(Vec<String>),
    Str(String),
}

impl Val {
    #[inline]
    pub fn add(&self, rhs: &Val) -> Result<Val, ()> {
        let val = match (self, rhs) {
            (&Val::IntVec(ref lhs), &Val::IntVec(ref rhs)) => Val::IntVec(vec_add(lhs, rhs)),
            (&Val::IntVec(ref lhs), &Val::Int(rhs)) => Val::IntVec(vec_scalar_add(lhs, rhs)),
            (&Val::Int(x), &Val::Int(y)) => Val::Int(x + y),
            (&Val::StrVec(ref lhs), &Val::StrVec(ref rhs)) => Val::StrVec(strs_add(lhs, rhs)),
            (&Val::Str(ref lhs), &Val::Str(ref rhs)) => Val::Str(str_add(lhs, rhs)),
            _ => unimplemented!(),
        };
        Ok(val)
    }

    #[inline]
    pub fn sub(&self, rhs: &Val) -> Result<Val, ()> {
        let val = match (self, rhs) {
            (&Val::IntVec(ref lhs), &Val::IntVec(ref rhs)) => Val::IntVec(vec_sub(lhs, rhs)),
            (&Val::IntVec(ref lhs), &Val::Int(rhs)) => Val::IntVec(vec_scalar_sub(lhs, rhs)),
            (&Val::Int(x), &Val::Int(y)) => Val::Int(x - y),
            _ => unimplemented!(),
        };
        Ok(val)
    }

    #[inline]
    pub fn mul(&self, rhs: &Val) -> Result<Val, ()> {
        let val = match (self, rhs) {
            (&Val::IntVec(ref lhs), &Val::IntVec(ref rhs)) => Val::IntVec(vec_mul(lhs, rhs)),
            (&Val::IntVec(ref lhs), &Val::Int(rhs)) => Val::IntVec(veci32_i32mul(lhs, rhs)),
            (&Val::Int(lhs), &Val::IntVec(ref rhs)) => Val::IntVec(vec_scalar_mul(rhs, lhs)),
            (&Val::Int(ref x), &Val::Int(y)) => Val::Int(x * y),
            _ => unimplemented!(),
        };
        Ok(val)
    }

    #[inline]
    pub fn div(&self, rhs: &Val) -> Result<Val, ()> {
        let val = match (self, rhs) {
            (&Val::IntVec(ref lhs), &Val::IntVec(ref rhs)) => Val::IntVec(vec_div(lhs, rhs)),
            (&Val::IntVec(ref lhs), &Val::Int(rhs)) => Val::IntVec(vec_scalar_div(lhs, rhs)),
            _ => unimplemented!(),
        };
        Ok(val)
    }

    #[inline]
    fn len(&self) -> usize {
        match *self {
            Val::Int(_) => 1,
            Val::IntVec(ref vec) => vec.len(),
            Val::Str(_) => 1,
            Val::StrVec(ref vec) => vec.len(),           
        }
    }

    #[inline]
    fn sum(&self) -> Result<Val, &'static str> {
        let val = match *self {
            Val::Int(val) => Val::Int(val),
            Val::IntVec(ref vec) => Val::Int(vec_sum(vec)),
            Val::Str(_) => return Err("cannot sum str"),
            Val::StrVec(_) => return Err("cannot sum strvec"),
        };
        Ok(val)
    }

    #[inline]
    fn sums(&self) -> Result<Val, &'static str> {
        let val = match *self {
            Val::Int(val) => Val::Int(val),
            Val::IntVec(ref vec) => Val::IntVec(vec_sums(vec)),
            Val::Str(_) => return Err("cannot sum str"),
            Val::StrVec(_) => return Err("cannot sum strvec"),
        };
        Ok(val)
    }

    #[inline]
    fn max(&self) -> Result<Val, &'static str> {
        let val = match *self {
            Val::Int(val) => Val::Int(val),
            Val::IntVec(ref vec) => Val::Int(vec_max(vec).unwrap()),//Val::Int(*vec.iter().max().unwrap()),
            _ => unimplemented!(),
        };
        Ok(val)
    }

    fn maxs(&self) -> Result<Val, &'static str> {
        let val = match *self {
            Val::Int(val) => Val::Int(val),
            Val::IntVec(ref vec) => Val::IntVec(vec_maxs(vec)),
            _ => unimplemented!(),
        };
        Ok(val)
    }

    fn min(&self) -> Result<Val, &'static str> {
        let val = match *self {
            Val::Int(val) => Val::Int(val),
            Val::IntVec(ref vec) => Val::Int(vec_min(vec).unwrap()),
            _ => unimplemented!(),
        };
        Ok(val)
    }

    fn mins(&self) -> Result<Val, &'static str> {
        let val = match *self {
            Val::Int(val) => Val::Int(val),
            Val::IntVec(ref vec) => Val::IntVec(vec_mins(vec)),
            _ => unimplemented!(),
        };
        Ok(val)
    }

    fn product(&self) -> Result<Val, &'static str> {
        let val = match *self {
            Val::Int(val) => Val::Int(val),
            Val::IntVec(ref vec) => Val::Int(vec.iter().product()),
            _ => unimplemented!(),
        };
        Ok(val)
    }

    fn products(&self) -> Result<Val, &'static str> {
        let val = match *self {
            Val::Int(val) => Val::Int(val),
            Val::IntVec(ref vec) => Val::IntVec(vec_products(vec)),
            _ => unimplemented!(),
        };
        Ok(val)
    }

    fn range(&self) -> Result<Val, &'static str> {
        match *self {
            Val::Int(val) => Ok(Val::Int(val)),
            Val::IntVec(ref vec) => Ok(Val::IntVec(vec_int_range(vec))),
            _ => unimplemented!(),
        }
    }

    #[inline]
    fn get(&self, pos: usize) -> Option<String> {
        match *self {
            Val::Int(val) if pos == 0 => Some(val.to_string()),
            Val::Int(_) => None,
            Val::IntVec(ref vec) => vec.get(pos).map(|x| x.to_string()),
            Val::Str(ref val) if pos == 0 => Some(val.to_string()),
            Val::Str(_) => None,
            Val::StrVec(ref vec) => vec.get(pos).map(|x| x.to_string()),
        }
    }

    #[inline]
    fn filter_gate(&self, pred: Predicate, val: Val) -> Vec<usize> {
        match (self, pred, val) {
            (&Val::IntVec(ref vec), Predicate::Equal, Val::Int(val)) => {
                vec.iter().enumerate().filter(|&(_,x)| *x == val).map(|(i,_)| i).collect()
            }
            (&Val::Int(val), Predicate::Equal, Val::IntVec(ref vec)) => {
                vec.iter().enumerate().filter(|&(_,x)| *x == val).map(|(i,_)| i).collect()
            }
            _ => unimplemented!()            
        }
    }
}

impl fmt::Display for Val {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Val::Int(val) => write!(f, "{}", val),
            Val::IntVec(ref vec) => write!(f, "{}", vec[0]),
            Val::Str(ref val) => write!(f, "{}", val),
            Val::StrVec(ref vec) => write!(f, "{}", vec[0]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;

    const COL_LEN: usize = 10;

    fn id_expr<S: Into<String>>(id: S) -> Expr {
        Expr::Id(id.into())
    }

    fn test_table<S: Into<String>>(id: S) -> Table {
        let n = COL_LEN;
        let a = Column::from("a", Val::IntVec(vec![1; n]));
        let b = Column::from("b", Val::IntVec(vec![1; n]));
        let c = Column::from("c", Val::IntVec(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
        let cols = vec![a, b, c];
        Table::from(id.into(), cols)
    }

    fn test_db() -> Database {
        let t1 = test_table("t1");
        let t2 = test_table("t2");
        let tables = vec![t1, t2];
        Database::from(tables)
    }

    #[test]
    fn val_add_intvecs() {
        let n = COL_LEN;
        let x = Val::IntVec(vec![1; n]);
        let y = Val::IntVec(vec![1; n]);
        assert_eq!(x.add(&y).unwrap(), Val::IntVec(vec![2; n]));
    }

    #[test]
    fn val_add_strvecs() {
        let n = COL_LEN;
        let x = Val::StrVec((0..n).map(|x| x.to_string()).collect());
        let y = Val::StrVec((0..n).map(|x| x.to_string()).collect());
        assert_eq!(
            x.add(&y).unwrap(),
            Val::StrVec((0..n).map(|x| x.to_string() + &x.to_string()).collect())
        );
    }

    #[test]
    fn val_add_intvec_scalar() {
        let n = COL_LEN;
        let x = Val::IntVec(vec![1; n]);
        let y = Val::Int(1);
        assert_eq!(x.add(&y).unwrap(), Val::IntVec(vec![2; n]));
    }

    #[test]
    fn col_add_intvec_intvec() {
        let n = COL_LEN;
        let a = Column::from("a", Val::IntVec(vec![1; n]));
        let b = Column::from("b", Val::IntVec(vec![1; n]));
        assert_eq!(
            a.add(&b).unwrap(),
            Column::from("a + b", Val::IntVec(vec![2; n]))
        );
    }

    #[test]
    fn col_sums() {
        let a = Column::from("a", Val::IntVec(vec![1; 5]));
        assert_eq!(
            a.sums().unwrap(),
            Column::from("sums(a)", Val::IntVec(vec![1, 2, 3, 4, 5]))
        );
    }

    #[test]
    fn table_get() {
        let n = COL_LEN;
        let a = Column::from("a", Val::IntVec(vec![1; n]));
        let b = Column::from("b", Val::IntVec(vec![1; n]));
        let c = Column::from("c", Val::IntVec(vec![1; n]));
        let cols = vec![a, b, c];
        let tbl = Table::from("t", cols);
        assert_eq!(
            tbl.get("b").unwrap(),
            &Column::from("b", Val::IntVec(vec![1; n]))
        );
    }

    #[test]
    fn db_get() {
        let db = test_db();
        let tbl = test_table("t2");
        assert_eq!(db.get("t2").unwrap(), &tbl);
    }

    #[test]
    fn id_expr_eval() {
        let tbl = test_table("t");
        let expr = Expr::Id(String::from("a"));
        let col = Column::from("a", Val::IntVec(vec![1; COL_LEN]));
        assert_eq!(expr.eval(&tbl).unwrap(), col);
    }

    #[test]
    fn max_expr_eval() {
        let tbl = test_table("t");
        let arg = Box::new(Expr::Id(String::from("c")));
        let expr = Expr::UnrFn(UnrOp::Max, arg);
        let col = Column::from("max(c)", Val::Int(10));
        assert_eq!(expr.eval(&tbl).unwrap(), col);
    }

    #[test]
    fn min_expr_eval() {
        let tbl = test_table("t");
        let arg = Box::new(Expr::Id(String::from("c")));
        let expr = Expr::UnrFn(UnrOp::Min, arg);
        let col = Column::from("min(c)", Val::Int(1));
        assert_eq!(expr.eval(&tbl).unwrap(), col);
    }

    #[test]
    fn col_add_expr_eval() {
        let tbl = test_table("t");
        let lhs = Box::new(id_expr("a"));
        let rhs = Box::new(id_expr("b"));
        let op = Expr::BinFn(lhs, BinOp::Add, rhs);
        let res = op.eval(&tbl).unwrap();
        let col = Column::from("a + b", Val::IntVec(vec![2; COL_LEN]));
        assert_eq!(res, col);
    }

    #[test]
    fn col_sub_expr_eval() {
        let tbl = test_table("t");
        let lhs = Box::new(id_expr("a"));
        let rhs = Box::new(id_expr("b"));
        let op = Expr::BinFn(lhs, BinOp::Sub, rhs);
        let res = op.eval(&tbl).unwrap();
        let col = Column::from("a - b", Val::IntVec(vec![0; COL_LEN]));
        assert_eq!(res, col);
    }

    #[test]
    fn query_select_from_exec() {
        let db = test_db();
        let qry = Query::from_str("select a+a, b-b from t1").unwrap();
        let col1 = Column::from("a + a", Val::IntVec(vec![2; COL_LEN]));
        let col2 = Column::from("b - b", Val::IntVec(vec![0; COL_LEN]));
        let exp = Table::from("t1", vec![col1, col2]);
        let tbl = qry.exec(&db).unwrap();
        assert_eq!(tbl, exp);
    }

    #[test]
    fn query_select_sum_exec() {
        let db = test_db();
        let qry = Query::from_str("select sum(a) from t1").unwrap();
        let col1 = Column::from("sum(a)", Val::Int(10));
        let exp = Table::from("t1", vec![col1]);
        let tbl = qry.exec(&db).unwrap();
        assert_eq!(tbl, exp);
    }

    #[test]
    fn query_select_aggs_from_exec() {
        let db = test_db();
        let qry = Query::from_str("select max(c), min(c), sum(a), product(c) from t1").unwrap();
        let col1 = Column::from("max(c)", Val::Int(10));
        let col2 = Column::from("min(c)", Val::Int(1));
        let col3 = Column::from("sum(a)", Val::Int(10));
        let col4 = Column::from("product(c)", Val::Int(3_628_800));
        let exp = Table::from("t1", vec![col1, col2, col3, col4]);
        let tbl = qry.exec(&db).unwrap();
        assert_eq!(tbl, exp);
    }

    #[test]
    fn val_intvec_sum() {
        let n = 10;
        let val = Val::IntVec(vec![1; n]);
        let sum = val.sum().unwrap();
        assert_eq!(sum, Val::Int(10));
    }

    #[test]
    fn val_int_sum() {
        let val = Val::Int(1);
        let sum = val.sum().unwrap();
        assert_eq!(sum, Val::Int(1));
    }

    #[test]
    fn val_intvec_sums() {
        let val = Val::IntVec(vec![1, 2, 3, 4, 5]);
        let sums = val.sums().unwrap();
        assert_eq!(sums, Val::IntVec(vec![1, 3, 6, 10, 15]));
    }

    #[test]
    fn val_int_sums() {
        let val = Val::Int(1);
        let sums = val.sum().unwrap();
        assert_eq!(sums, Val::Int(1));
    }

    #[test]
    fn val_intvec_max() {
        let vec = vec![1, 2, 3, 4, 5];
        let val = Val::IntVec(vec);
        let max = val.max().unwrap();
        assert_eq!(max, Val::Int(5));
    }

    #[test]
    fn val_intvec_min() {
        let vec = vec![1, 2, 3, 4, 5];
        let val = Val::IntVec(vec);
        let min = val.min().unwrap();
        assert_eq!(min, Val::Int(1));
    }

    #[test]
    fn val_int_maxs() {
        let val = Val::Int(1);
        let maxs = val.maxs().unwrap();
        assert_eq!(maxs, Val::Int(1));
    }

    #[test]
    fn val_intvec_maxs() {
        let val = Val::IntVec(vec![1, 2, 1, 3, 1, 4]);
        let maxs = val.maxs().unwrap();
        assert_eq!(maxs, Val::IntVec(vec![1, 2, 2, 3, 3, 4]));
    }

    #[test]
    fn val_int_mins() {
        let val = Val::Int(1);
        let mins = val.mins().unwrap();
        assert_eq!(mins, Val::Int(1));
    }

    #[test]
    fn val_intvec_mins() {
        let val = Val::IntVec(vec![1, 2, 1, 3, 0, 4]);
        let mins = val.mins().unwrap();
        assert_eq!(mins, Val::IntVec(vec![1, 1, 1, 1, 0, 0]));
    }

    #[test]
    fn val_int_products() {
        let val = Val::Int(1);
        let res = val.mins().unwrap();
        assert_eq!(res, Val::Int(1));
    }

    #[test]
    fn val_intvec_products() {
        let val = Val::IntVec(vec![1, 2, 3, 4, 5]);
        let res = val.products().unwrap();
        assert_eq!(res, Val::IntVec(vec![1, 2, 6, 24, 120]));
    }

    #[test]
    fn val_intvec_product() {
        let val = Val::IntVec(vec![1, 2, 3, 4, 5]);
        let res = val.product().unwrap();
        assert_eq!(res, Val::Int(120));
    }

    #[test]
    fn val_int_product() {
        let val = Val::Int(123);
        let res = val.product().unwrap();
        assert_eq!(res, Val::Int(123));
    }

    #[test]
    fn val_strs_add() {
        let lhs = Val::Str(String::from("a1"));
        let rhs = Val::Str(String::from("b2"));
        assert_eq!(lhs.add(&rhs).unwrap(), Val::Str(String::from("a1b2")));
    }

    #[test]
    fn range_unr_expr_int_eval() {
        let arg = Box::new(Expr::Int(5));
        let expr = Expr::UnrFn(UnrOp::Range, arg);
        let tbl = test_table("t");
        let col = Column::from("range(5)", Val::IntVec(vec![0, 1, 2, 3, 4]));
        assert_eq!(expr.eval(&tbl).unwrap(), col);
    }

    #[test]
    fn range_bin_expr_int_int_eval() {
        let lhs = Box::new(Expr::Int(1));
        let rhs = Box::new(Expr::Int(5));
        let expr = Expr::BinFn(lhs, BinOp::Range, rhs);
        let tbl = test_table("t");
        let col = Column::from("range(1, 5)", Val::IntVec(vec![1, 2, 3, 4]));
        assert_eq!(expr.eval(&tbl).unwrap(), col);
    }

    #[test]
    fn operator_precedence() {
        let query = Query::from_str("select 1+2*3,1*2+3 from t1").unwrap();
        let db = test_db();
        let col1 = Column::from("1 + 2 * 3", Val::Int(7));
        let col2 = Column::from("1 * 2 + 3", Val::Int(5));
        let tbl = Table::from("t1", vec![col1, col2]);
        assert_eq!(query.exec(&db).unwrap(), tbl);
    }

    #[test]
    fn range_val_intvec() {
        let val = Val::IntVec(vec![5i32, 4, 1, 8, 10]);
        let actual = val.range().unwrap();
        assert_eq!(actual, Val::IntVec(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
    }

    #[test]
    fn column_intvec_range() {
        let col = Column::from("a", Val::IntVec(vec![5i32, 4, 1, 8, 10]));
        let exp = Column::from("range(a)", Val::IntVec(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
        assert_eq!(col.range().unwrap(), exp);
    }

    #[test]
    fn column_int_range() {
        let col = Column::from("a", Val::Int(10));
        let exp = Column::from("range(a)", Val::Int(10));
        assert_eq!(col.range().unwrap(), exp);
    }

    #[test]
    fn eq_filter_col_1_range() {
        let filter = Filter::new(Expr::Id(String::from("c")), Predicate::Equal, Expr::Int(1));
        let tbl = test_table("t");
        let gate = filter.apply(&tbl).unwrap();
        let expected = vec![0]; 
        assert_eq!(gate, expected);
    }    

    #[bench]
    fn bench_vec_iter_max(b: &mut Bencher) {
        let n = 5;
        let v: Vec<i32> = (0..n).map(|x| x as i32).collect();
        b.iter(|| v.iter().max().unwrap())
    }

    #[bench]
    fn bench_vec_new_iter_max(b: &mut Bencher) {
        let n = 5;
        let v: Vec<i32> = (0..n).map(|x| x as i32).collect();
        b.iter(|| vec_max_iter(&v))
    }

    #[bench]
    fn bench_vec_max(b: &mut Bencher) {
        let n = 5;
        let v: Vec<i32> = (0..n).map(|x| x as i32).collect();
        b.iter(|| vec_max(&v))
    }
}
