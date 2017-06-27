use std::cmp;
use std::ops::{Add, Sub, Mul, Div};

use parser::Parser;

#[derive(Debug, PartialEq)]
pub enum Expr {
    Id(String),
    Int(i64),
    UnrFn(UnrOp, Box<Expr>),
    BinFn(Box<Expr>, BinOp, Box<Expr>),
}

impl Expr {
    fn eval(&self, tbl: &Table) -> Result<Column, &'static str> {
        match *self {
            Expr::Id(ref id) => {
                match tbl.get(id) {
                    Some(col) => Ok(col.clone()),
                    None => Err("cannot find col"),
                }
            }
            Expr::Int(val) => Ok(Column::from("c", Val::Int(val))),
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
            Expr::BinFn(box Expr::Id(ref id1), BinOp::Add, box Expr::Id(ref id2)) => {
                let (lhs, rhs) = match tbl.get_2(id1, id2) {
                    Some(cols) => cols,
                    None => return Err("cannot find cols"),
                };
                lhs.add(rhs).map_err(|_| "cannot evaluate col add")
            }
            Expr::BinFn(box Expr::Id(ref id1), BinOp::Sub, box Expr::Id(ref id2)) => {
                let (lhs, rhs) = match tbl.get_2(id1, id2) {
                    Some(cols) => cols,
                    None => return Err("cannot find cols"),
                };
                lhs.sub(rhs).map_err(|_| "cannot evaluate col sub")
            }    
            Expr::BinFn(box Expr::Id(ref id1), BinOp::Mul, box Expr::Id(ref id2)) => {
                let (lhs, rhs) = match tbl.get_2(id1, id2) {
                    Some(cols) => cols,
                    None => return Err("cannot find cols"),
                };
                lhs.mul(rhs).map_err(|_| "cannot evaluate col sub")
            }    
            Expr::BinFn(box Expr::Id(ref id1), BinOp::Div, box Expr::Id(ref id2)) => {
                let (lhs, rhs) = match tbl.get_2(id1, id2) {
                    Some(cols) => cols,
                    None => return Err("cannot find cols"),
                };
                lhs.div(rhs).map_err(|_| "cannot evaluate col sub")
            }                                    
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum BinOp {
    Add,
    Sub,
    Mul, 
    Div,
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
}

#[derive(Debug, PartialEq)]
pub struct Query {
    select: Vec<Expr>,
    by: Option<Vec<Expr>>,
    from: Expr,
    filters: Option<Vec<Expr>>,
}

impl Query {
    pub fn from(
        select: Vec<Expr>,
        by: Option<Vec<Expr>>,
        from: Expr,
        filters: Option<Vec<Expr>>,
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
        for expr in &self.select {
            match expr.eval(tbl) {
                Ok(col) => cols.push(col),
                Err(err) => return Err(err),
            }
        }
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

    pub fn get(&self, name: &str) -> Option<&Table> {
        self.tables.iter().find(|tbl| tbl.name == name)
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
        Table {
            name: name.into(),
            cols: cols,
            len: 100,
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
}

#[derive(Debug, PartialEq, Clone)]
pub enum Val {
    IntVec(Vec<i64>),
    Int(i64),
    StrVec(Vec<String>),
    Str(String),
}

impl Val {
    pub fn add(&self, rhs: &Val) -> Result<Val, ()> {
        let val = match (self, rhs) {
            (&Val::IntVec(ref lhs), &Val::IntVec(ref rhs)) => Val::IntVec(vec_add(lhs, rhs)),
            (&Val::IntVec(ref lhs), &Val::Int(rhs)) => Val::IntVec(vec_scalar_add(lhs, rhs)),
            (&Val::StrVec(ref lhs), &Val::StrVec(ref rhs)) => Val::StrVec(strs_add(lhs, rhs)),
            _ => return Err(()),
        };
        Ok(val)
    }

    pub fn sub(&self, rhs: &Val) -> Result<Val, ()> {
        let val = match (self, rhs) {
            (&Val::IntVec(ref lhs), &Val::IntVec(ref rhs)) => Val::IntVec(vec_sub(lhs, rhs)),
            (&Val::IntVec(ref lhs), &Val::Int(rhs)) => Val::IntVec(vec_scalar_sub(lhs, rhs)),
            _ => return Err(()),
        };
        Ok(val)
    }

    pub fn mul(&self, rhs: &Val) -> Result<Val, ()> {
        let val = match (self, rhs) {
            (&Val::IntVec(ref lhs), &Val::IntVec(ref rhs)) => Val::IntVec(vec_mul(lhs, rhs)),
            (&Val::IntVec(ref lhs), &Val::Int(rhs)) => Val::IntVec(vec_scalar_mul(lhs, rhs)),
            _ => return Err(()),
        };
        Ok(val)
    }    

    pub fn div(&self, rhs: &Val) -> Result<Val, ()> {
        let val = match (self, rhs) {
            (&Val::IntVec(ref lhs), &Val::IntVec(ref rhs)) => Val::IntVec(vec_div(lhs, rhs)),
            (&Val::IntVec(ref lhs), &Val::Int(rhs)) => Val::IntVec(vec_scalar_div(lhs, rhs)),
            _ => return Err(()),
        };
        Ok(val)
    } 

    fn sum(&self) -> Result<Val, &'static str> {
        let val = match *self {
            Val::Int(val) => Val::Int(val),
            Val::IntVec(ref vec) => Val::Int(vec.iter().sum()),
            Val::Str(_) => return Err("cannot sum str"),
            Val::StrVec(_) => return Err("cannot sum strvec"),
        };
        Ok(val)
    }

    fn sums(&self) -> Result<Val, &'static str> {
        let val = match *self {
            Val::Int(val) => Val::Int(val),
            Val::IntVec(ref vec) => Val::IntVec(vec_sums(vec)),
            Val::Str(_) => return Err("cannot sum str"),
            Val::StrVec(_) => return Err("cannot sum strvec"),
        };
        Ok(val)
    }    

    fn max(&self) -> Result<Val, &'static str> {
        let val = match *self {
            Val::Int(val) => Val::Int(val),
            Val::IntVec(ref vec) => Val::Int(*vec.iter().max().unwrap()),
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
            Val::IntVec(ref vec) => Val::Int(*vec.iter().min().unwrap()),
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
}

#[inline]
fn vec_add<T: Add<Output = T> + Copy>(a: &[T], b: &[T]) -> Vec<T> {
    a.iter().zip(b.iter()).map(|(x, y)| *x + *y).collect()
}

#[inline]
fn vec_sub<T: Sub<Output = T> + Copy>(a: &[T], b: &[T]) -> Vec<T> {
    a.iter().zip(b.iter()).map(|(x, y)| *x - *y).collect()
}

#[inline]
fn vec_mul<T: Mul<Output = T> + Copy>(a: &[T], b: &[T]) -> Vec<T> {
    a.iter().zip(b.iter()).map(|(x, y)| *x * *y).collect()
}

#[inline]
fn vec_div<T: Div<Output = T> + Copy>(a: &[T], b: &[T]) -> Vec<T> {
    a.iter().zip(b.iter()).map(|(x, y)| *x / *y).collect()
}

#[inline]
fn vec_maxs<T: Ord + Copy>(v: &[T]) -> Vec<T> {
    assert!(!v.is_empty());
    let mut max = v[0];
    let mut maxs = Vec::with_capacity(v.len());
    maxs.push(max);
    for val in &v[1..] {
        max = cmp::max(max, *val);
        maxs.push(max);
    }
    maxs
}

#[inline]
fn vec_products<T: Ord + Copy + Mul<Output=T>>(v: &[T]) -> Vec<T> {
    assert!(!v.is_empty());
    let mut product = v[0];
    let mut products = Vec::with_capacity(v.len());
    products.push(product);
    for val in &v[1..] {
        product = product * (*val);
        products.push(product);
    }
    products
}

#[inline]
fn vec_sums<T: Ord + Copy + Add<Output=T>>(v: &[T]) -> Vec<T> {
    assert!(!v.is_empty());
    let mut sum = v[0];
    let mut sums = Vec::with_capacity(v.len());
    sums.push(sum);
    for val in &v[1..] {
        sum = sum + (*val);
        sums.push(sum);
    }
    sums
}

#[inline]
fn vec_mins<T: Ord + Copy>(v: &[T]) -> Vec<T> {
    assert!(!v.is_empty());
    let mut min = v[0];
    let mut mins = Vec::with_capacity(v.len());
    mins.push(min);
    for val in &v[1..] {
        min = cmp::min(min, *val);
        mins.push(min);
    }
    mins
}

#[inline]
fn vec_scalar_sub<T: Sub<Output = T> + Copy>(a: &[T], b: T) -> Vec<T> {
    a.iter().map(|x| *x - b).collect()
}

#[inline]
fn vec_scalar_add<T: Add<Output = T> + Copy>(a: &[T], b: T) -> Vec<T> {
    a.iter().map(|x| *x + b).collect()
}

#[inline]
fn vec_scalar_mul<T: Mul<Output = T> + Copy>(a: &[T], b: T) -> Vec<T> {
    a.iter().map(|x| *x * b).collect()
}

#[inline]
fn vec_scalar_div<T: Div<Output = T> + Copy>(a: &[T], b: T) -> Vec<T> {
    a.iter().map(|x| *x / b).collect()
}

fn strs_add(a: &[String], b: &[String]) -> Vec<String> {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| x.to_string() + y)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const COL_LEN: usize = 10;

    fn id_expr<S: Into<String>>(id: S) -> Expr {
        Expr::Id(id.into())
    }

    fn test_table<S: Into<String>>(id: S) -> Table {
        let n = COL_LEN;
        let a = Column::from("a", Val::IntVec(vec![1; n]));
        let b = Column::from("b", Val::IntVec(vec![1; n]));
        let c = Column::from("c", Val::IntVec(vec![1,2,3,4,5,6,7,8,9,10]));
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
    fn val_int_sum() {
        let val = Val::Int(1);
        let sum = val.sum().unwrap();
        assert_eq!(sum, Val::Int(1));
    }
}
