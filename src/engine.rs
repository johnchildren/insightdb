use std::collections::{BTreeMap, BinaryHeap};
use std::fmt;

use parser::Parser;
use tables::*;
use computation::*;
use databases::InMemoryDb;
use aggregators::*;

use chrono::prelude::*;

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

    fn apply(&self, table: &InMemoryTable) -> Result<Vec<usize>, &'static str> {
        match (&self.lhs, &self.op, &self.rhs) {
            (&Expr::Id(ref id), op, &Expr::Int(val)) => {
                let col = table.get(id)?;
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
    fn eval<'b>(&self, tbl: &'b InMemoryTable) -> Result<InMemoryColumn, &'static str> {
        match *self {
            Expr::Id(ref id) => tbl.get(id).map(|col| col.clone()),
            Expr::Str(ref s) => {
                let name = s.clone();
                let val = Val::Str(s.clone());
                Ok(InMemoryColumn::from(name, val))
            }
            Expr::Int(val) => Ok(InMemoryColumn::from(val.to_string(), Val::Int(val))),
            Expr::UnrFn(UnrOp::Til, box Expr::Int(n)) => {
                let name = "til(".to_string() + &n.to_string() + ")";
                let val = Val::IntVec(ranged_vec(0, n as usize));
                Ok(InMemoryColumn::from(name, val))
            }
            Expr::UnrFn(UnrOp::Sum, box Expr::UnrFn(UnrOp::Til, box Expr::Int(n))) => {
                println!("inside");
                let name = "sum(til(".to_string() + &n.to_string() + "))";
                let val = Val::Int(n * (n + 1) / 2);
                Ok(InMemoryColumn::from(name, val))
            }            
            Expr::UnrFn(UnrOp::Sum, box Expr::UnrFn(UnrOp::Range, box Expr::Int(n))) => {
                let name = "sum(range(".to_string() + &n.to_string() + "))";
                let val = Val::Int(n * (n + 1) / 2);
                Ok(InMemoryColumn::from(name, val))
            }
            Expr::UnrFn(UnrOp::Sum, box Expr::Id(ref id)) => {
                let col = tbl.get(id)?;
                return col.sum();
            }
            Expr::UnrFn(UnrOp::Sums, box Expr::Id(ref id)) => {
                let col = tbl.get(id)?;
                return col.sums();
            }            
            Expr::UnrFn(UnrOp::Min, box Expr::Id(ref id)) => {
                let col = tbl.get(id)?;
                return col.min();
            }   
            Expr::UnrFn(UnrOp::Mins, box Expr::Id(ref id)) => {
                let col = tbl.get(id)?;
                return col.mins();
            }               
            Expr::UnrFn(UnrOp::Max, box Expr::Id(ref id)) => {
                let col = tbl.get(id)?;
                return col.max();
            } 
            Expr::UnrFn(UnrOp::Maxs, box Expr::Id(ref id)) => {
                let col = tbl.get(id)?;
                return col.maxs();
            }             
            Expr::UnrFn(UnrOp::Product, box Expr::Id(ref id)) => {
                let col = tbl.get(id)?;
                return col.product();
            }  
            Expr::UnrFn(UnrOp::Products, box Expr::Id(ref id)) => {
                let col = tbl.get(id)?;
                return col.products();
            }    
            Expr::UnrFn(UnrOp::Range, box Expr::Int(n)) => {
                let val = Val::IntVec(ranged_vec(0, n as usize));
                let name = "range(".to_string() + &n.to_string() + ")";
                Ok(InMemoryColumn::from(name, val))
            } 
            Expr::UnrFn(UnrOp::Range, box Expr::Id(ref id)) => {
                let col = tbl.get(id).unwrap();
                col.range()
            }  
             
            Expr::UnrFn(UnrOp::Unique, box Expr::Id(ref id)) => {
                let col = tbl.get(id).unwrap();
                col.unique()
            }  
            Expr::UnrFn(UnrOp::Unique, ref expr) => {
                let col = expr.eval(tbl)?;
                col.unique()
            }                                                                                                
            Expr::BinFn(box Expr::Int(lhs), BinOp::Range, box Expr::Int(rhs)) => {
                let name = "range(".to_string() + &lhs.to_string() + ", " + &rhs.to_string() + ")";
                let val = Val::IntVec(ranged_vec(lhs as usize, rhs as usize));
                return Ok(InMemoryColumn::from(name, val));
            }
            Expr::BinFn(box Expr::Str(ref lhs), BinOp::Add, box Expr::Str(ref rhs)) => {
                let name = "\"".to_string() + lhs + "\" + \"" + rhs + "\"";
                let val = lhs.to_string() + &rhs;
                return Ok(InMemoryColumn::from(name, Val::Str(val)));
            }
            Expr::BinFn(box Expr::Id(ref id1), BinOp::Add, box Expr::Id(ref id2)) => {
                let lhs = tbl.get(id1)?;
                let rhs = tbl.get(id2)?;
                lhs.add(rhs).map_err(|_| "cannot evaluate col add")
            }
            Expr::BinFn(ref lhs, BinOp::Add, ref rhs) => {
                let lcol = lhs.eval(tbl)?;
                let rcol = rhs.eval(tbl)?;
                return lcol.add(&rcol).map_err(|_| "cannot add exprs");
            }   
            Expr::BinFn(box Expr::Id(ref id1), BinOp::Sub, box Expr::Id(ref id2)) => {
                let lhs = tbl.get(id1)?;
                let rhs = tbl.get(id2)?;
                lhs.sub(rhs).map_err(|_| "cannot evaluate col sub")
            }              
            Expr::BinFn(ref lhs, BinOp::Sub, ref rhs) => {
                let lcol = lhs.eval(tbl)?;
                let rcol = rhs.eval(tbl)?;
                return lcol.sub(&rcol).map_err(|_| "cannot sub exprs");
            }    
            Expr::BinFn(box Expr::Id(ref id1), BinOp::Mul, box Expr::Id(ref id2)) => {
                let lhs = tbl.get(id1)?;
                let rhs = tbl.get(id2)?;
                lhs.mul(rhs).map_err(|_| "cannot evaluate col sub")
            }                 
            Expr::BinFn(ref lhs, BinOp::Mul, ref rhs) => {
                let lcol = lhs.eval(tbl)?;
                let rcol = rhs.eval(tbl)?;
                return lcol.mul(&rcol).map_err(|_| "cannot mul exprs");
            }                                    
            Expr::BinFn(box Expr::Id(ref id1), BinOp::Div, box Expr::Id(ref id2)) => {
                let lhs = tbl.get(id1)?;
                let rhs = tbl.get(id2)?;
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
    Product,
    Products,
    Range,
    Til,
    Unique,
    Deltas,
}

#[derive(Debug, PartialEq)]
pub struct Query {
    select: Vec<Expr>,
    by: Option<Vec<Expr>>,
    from: Expr,
    filters: Option<Vec<Filter>>,
}

impl Query {
    #[inline]
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

    #[inline]
    pub fn from_str(cmd: &str) -> Result<Self, &'static str> {
        let mut parser = Parser::new(cmd);
        parser.parse()
    }

    #[inline]
    pub fn has_groupings(&self) -> bool {
        if let Some(ref bys) = self.by {
            return !bys.is_empty();
        }
        false
    }

    fn table<'a>(&self, db: &'a InMemoryDb) -> Result<&'a InMemoryTable, &'static str> {
        let tbl_id = match self.from {
            Expr::Id(ref id) => id,
            _ => return Err("unexpteced from expr"),
        };
        match db.get(tbl_id) {
            Some(tbl) => Ok(tbl),
            None => Err("cannot find table"),
        }
    }

    pub fn cols(&self, table: &InMemoryTable) -> Result<Vec<InMemoryColumn>, &'static str> {
        let mut cols = Vec::new();
        for expr in &self.select {
            match expr.eval(table) {
                Ok(col) => cols.push(col),
                Err(err) => return Err(err),
            }
        }
        Ok(cols)
    }

    #[inline]
    pub fn exec(&self, db: &InMemoryDb) -> Result<InMemoryTable, &'static str> {
        let ref_table = self.table(db)?;
        let cols = self.cols(ref_table)?;
        Ok(InMemoryTable::from(ref_table.name(), cols))
    }

    #[inline]
    pub fn exec_keyed(&self, db: &InMemoryDb) -> Result<KeyedTable, &'static str> {
        let ref_table = self.table(db)?;
        let by_col = match self.by {
            Some(ref exprs) => exprs.get(0).unwrap().eval(ref_table)?,
            None => return Err(""),
        };
        let by_vec = by_col.int_vec()?;
        let col = ref_table.get("a")?;
        let col_vec = col.int_vec()?;
        // aggregations
        let mut builder = KeyedTableBuilder::new();
        for (key, val) in by_vec.iter().zip(col_vec.iter()) {
            builder.push(*key, *val);
        }
        // convert aggregations to keyed table        
        Ok(builder.build())
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
    pub fn len(&self) -> usize {
        match *self {
            Val::Int(_) => 1,
            Val::IntVec(ref vec) => vec.len(),
            Val::Str(_) => 1,
            Val::StrVec(ref vec) => vec.len(),           
        }
    }

    #[inline]
    pub fn sum(&self) -> Result<Val, &'static str> {
        let val = match *self {
            Val::Int(val) => Val::Int(val),
            Val::IntVec(ref vec) => Val::Int(vec_sum(vec)),
            Val::Str(_) => return Err("cannot sum str"),
            Val::StrVec(_) => return Err("cannot sum strvec"),
        };
        Ok(val)
    }

    #[inline]
    pub fn sums(&self) -> Result<Val, &'static str> {
        let val = match *self {
            Val::Int(val) => Val::Int(val),
            Val::IntVec(ref vec) => Val::IntVec(vec_sums(vec)),
            Val::Str(_) => return Err("cannot sum str"),
            Val::StrVec(_) => return Err("cannot sum strvec"),
        };
        Ok(val)
    }

    #[inline]
    pub fn max(&self) -> Result<Val, &'static str> {
        let val = match *self {
            Val::Int(val) => Val::Int(val),
            Val::IntVec(ref vec) => Val::Int(vec_max(vec).unwrap()),//Val::Int(*vec.iter().max().unwrap()),
            _ => unimplemented!(),
        };
        Ok(val)
    }

    #[inline]
    pub fn maxs(&self) -> Result<Val, &'static str> {
        let val = match *self {
            Val::Int(val) => Val::Int(val),
            Val::IntVec(ref vec) => Val::IntVec(vec_maxs(vec)),
            _ => unimplemented!(),
        };
        Ok(val)
    }

    #[inline]
    pub fn min(&self) -> Result<Val, &'static str> {
        let val = match *self {
            Val::Int(val) => Val::Int(val),
            Val::IntVec(ref vec) => Val::Int(vec_min(vec).unwrap()),
            _ => unimplemented!(),
        };
        Ok(val)
    }

    #[inline]
    pub fn mins(&self) -> Result<Val, &'static str> {
        let val = match *self {
            Val::Int(val) => Val::Int(val),
            Val::IntVec(ref vec) => Val::IntVec(vec_mins(vec)),
            _ => unimplemented!(),
        };
        Ok(val)
    }

    #[inline]
    pub fn product(&self) -> Result<Val, &'static str> {
        let val = match *self {
            Val::Int(val) => Val::Int(val),
            Val::IntVec(ref vec) => Val::Int(vec.iter().product()),
            _ => unimplemented!(),
        };
        Ok(val)
    }

    #[inline]
    pub fn products(&self) -> Result<Val, &'static str> {
        let val = match *self {
            Val::Int(val) => Val::Int(val),
            Val::IntVec(ref vec) => Val::IntVec(vec_products(vec)),
            _ => unimplemented!(),
        };
        Ok(val)
    }

    #[inline]
    pub fn range(&self) -> Result<Val, &'static str> {
        match *self {
            Val::Int(val) => Ok(Val::Int(val)),
            Val::IntVec(ref vec) => Ok(Val::IntVec(vec_int_range(vec))),
            _ => unimplemented!(),
        }
    }

    #[inline]
    pub fn unique(&self) -> Result<Val, &'static str> {
        match *self {
            Val::Int(val) => Ok(Val::Int(val)),
            Val::IntVec(ref vec) => Ok(Val::IntVec(unique(vec))),
            Val::Str(ref str) => Ok(Val::Str(str.clone())),
            Val::StrVec(ref vec) => Ok(Val::StrVec(unique(vec))),
        }
    }    

    #[inline]
    pub fn get(&self, pos: usize) -> Option<String> {
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
    pub fn filter_gate(&self, pred: Predicate, val: Val) -> Vec<usize> {
        match (self, pred, val) {
            (&Val::IntVec(ref vec), Predicate::Equal, Val::Int(val)) |
            (&Val::Int(val), Predicate::Equal, Val::IntVec(ref vec)) => {
                vec.iter()
                    .enumerate()
                    .filter(|&(_, x)| *x == val)
                    .map(|(i, _)| i)
                    .collect()
            }         
            (&Val::IntVec(ref vec), Predicate::Less, Val::Int(val)) => {
                vec.iter()
                    .enumerate()
                    .filter(|&(_, x)| *x < val)
                    .map(|(i, _)| i)
                    .collect()
            }   
            (&Val::IntVec(ref vec), Predicate::LessEqual, Val::Int(val)) => {
                vec.iter()
                    .enumerate()
                    .filter(|&(_, x)| *x <= val)
                    .map(|(i, _)| i)
                    .collect()
            }    
            (&Val::IntVec(ref vec), Predicate::Greater, Val::Int(val)) => {
                vec.iter()
                    .enumerate()
                    .filter(|&(_, x)| *x > val)
                    .map(|(i, _)| i)
                    .collect()
            }   
            (&Val::IntVec(ref vec), Predicate::GreaterEqual, Val::Int(val)) => {
                vec.iter()
                    .enumerate()
                    .filter(|&(_, x)| *x >= val)
                    .map(|(i, _)| i)
                    .collect()
            }                                             
            _ => unimplemented!(),            
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
    use std::collections::HashSet;
    use rayon::prelude::ParallelSliceMut;

    const COL_LEN: usize = 10;

    fn id_expr<S: Into<String>>(id: S) -> Expr {
        Expr::Id(id.into())
    }

    fn test_table<S: Into<String>>(id: S, n: usize) -> InMemoryTable {
        let a = InMemoryColumn::from("a", Val::IntVec(vec![1; n]));
        let b = InMemoryColumn::from("b", Val::IntVec(vec![1; n]));
        let seq: Vec<i32> = (0..n).map(|x| (x + 1) as i32).collect();
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

    fn str_expr<S: Into<String>>(s: S) -> Expr {
        Expr::Str(s.into())
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
        let a = InMemoryColumn::from("a", Val::IntVec(vec![1; n]));
        let b = InMemoryColumn::from("b", Val::IntVec(vec![1; n]));
        assert_eq!(
            a.add(&b).unwrap(),
            InMemoryColumn::from("a + b", Val::IntVec(vec![2; n]))
        );
    }

    #[test]
    fn col_sums() {
        let a = InMemoryColumn::from("a", Val::IntVec(vec![1; 5]));
        assert_eq!(
            a.sums().unwrap(),
            InMemoryColumn::from("sums(a)", Val::IntVec(vec![1, 2, 3, 4, 5]))
        );
    }

    #[test]
    fn table_get() {
        let n = COL_LEN;
        let a = InMemoryColumn::from("a", Val::IntVec(vec![1; n]));
        let b = InMemoryColumn::from("b", Val::IntVec(vec![1; n]));
        let c = InMemoryColumn::from("c", Val::IntVec(vec![1; n]));
        let cols = vec![a, b, c];
        let tbl = InMemoryTable::from("t", cols);
        assert_eq!(
            tbl.get("b").unwrap(),
            &InMemoryColumn::from("b", Val::IntVec(vec![1; n]))
        );
    }

    #[test]
    fn id_expr_eval() {
        let tbl = test_table("t", COL_LEN);
        let expr = Expr::Id(String::from("a"));
        let col = InMemoryColumn::from("a", Val::IntVec(vec![1; COL_LEN]));
        assert_eq!(expr.eval(&tbl).unwrap(), col);
    }

    #[test]
    fn max_expr_eval() {
        let tbl = test_table("t", COL_LEN);
        let arg = Box::new(Expr::Id(String::from("c")));
        let expr = Expr::UnrFn(UnrOp::Max, arg);
        let col = InMemoryColumn::from("max(c)", Val::Int(10));
        assert_eq!(expr.eval(&tbl).unwrap(), col);
    }

    #[test]
    fn min_expr_eval() {
        let tbl = test_table("t", COL_LEN);
        let arg = Box::new(Expr::Id(String::from("c")));
        let expr = Expr::UnrFn(UnrOp::Min, arg);
        let col = InMemoryColumn::from("min(c)", Val::Int(1));
        assert_eq!(expr.eval(&tbl).unwrap(), col);
    }

    #[test]
    fn col_add_expr_eval() {
        let tbl = test_table("t", COL_LEN);
        let lhs = Box::new(id_expr("a"));
        let rhs = Box::new(id_expr("b"));
        let op = Expr::BinFn(lhs, BinOp::Add, rhs);
        let res = op.eval(&tbl).unwrap();
        let col = InMemoryColumn::from("a + b", Val::IntVec(vec![2; COL_LEN]));
        assert_eq!(res, col);
    }

    #[test]
    fn col_sub_expr_eval() {
        let tbl = test_table("t", COL_LEN);
        let lhs = Box::new(id_expr("a"));
        let rhs = Box::new(id_expr("b"));
        let op = Expr::BinFn(lhs, BinOp::Sub, rhs);
        let res = op.eval(&tbl).unwrap();
        let col = InMemoryColumn::from("a - b", Val::IntVec(vec![0; COL_LEN]));
        assert_eq!(res, col);
    }

    #[test]
    fn query_select_from_exec() {
        let n = 10;
        let db = test_db(n);
        let qry = Query::from_str("select a+a, b-b from t1").unwrap();
        let col1 = InMemoryColumn::from("a + a", Val::IntVec(vec![2; COL_LEN]));
        let col2 = InMemoryColumn::from("b - b", Val::IntVec(vec![0; COL_LEN]));
        let exp = InMemoryTable::from("t1", vec![col1, col2]);
        let tbl = qry.exec(&db).unwrap();
        assert_eq!(tbl, exp);
    }

    #[test]
    fn query_select_sum_exec() {
        let n = 10;
        let db = test_db(n);
        let qry = Query::from_str("select sum(a) from t1").unwrap();
        let col1 = InMemoryColumn::from("sum(a)", Val::Int(10));
        let exp = InMemoryTable::from("t1", vec![col1]);
        let tbl = qry.exec(&db).unwrap();
        assert_eq!(tbl, exp);
    }

    #[test]
    fn query_select_aggs_from_exec() {
        let n = 10;
        let db = test_db(n);
        let qry = Query::from_str("select max(c), min(c), sum(a), product(c) from t1").unwrap();
        let col1 = InMemoryColumn::from("max(c)", Val::Int(10));
        let col2 = InMemoryColumn::from("min(c)", Val::Int(1));
        let col3 = InMemoryColumn::from("sum(a)", Val::Int(10));
        let col4 = InMemoryColumn::from("product(c)", Val::Int(3_628_800));
        let exp = InMemoryTable::from("t1", vec![col1, col2, col3, col4]);
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
        let tbl = test_table("t", 5);
        let col = InMemoryColumn::from("range(5)", Val::IntVec(vec![0, 1, 2, 3, 4]));
        assert_eq!(expr.eval(&tbl).unwrap(), col);
    }

    #[test]
    fn range_bin_expr_int_int_eval() {
        let lhs = Box::new(Expr::Int(1));
        let rhs = Box::new(Expr::Int(5));
        let expr = Expr::BinFn(lhs, BinOp::Range, rhs);
        let tbl = test_table("t", 4);
        let col = InMemoryColumn::from("range(1, 5)", Val::IntVec(vec![1, 2, 3, 4]));
        assert_eq!(expr.eval(&tbl).unwrap(), col);
    }

    #[test]
    fn operator_precedence() {
        let query = Query::from_str("select 1+2*3,1*2+3 from t1").unwrap();
        let n = 10;
        let db = test_db(n);
        let col1 = InMemoryColumn::from("1 + 2 * 3", Val::Int(7));
        let col2 = InMemoryColumn::from("1 * 2 + 3", Val::Int(5));
        let table = InMemoryTable::from("t1", vec![col1, col2]);
        let result: InMemoryTable = query.exec(&db).unwrap();
        assert_eq!(result, table);
    }

    #[test]
    fn range_val_intvec() {
        let val = Val::IntVec(vec![5i32, 4, 1, 8, 10]);
        let actual = val.range().unwrap();
        assert_eq!(actual, Val::IntVec(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
    }

    #[test]
    fn column_intvec_range() {
        let col = InMemoryColumn::from("a", Val::IntVec(vec![5i32, 4, 1, 8, 10]));
        let exp =
            InMemoryColumn::from("range(a)", Val::IntVec(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
        assert_eq!(col.range().unwrap(), exp);
    }

    #[test]
    fn column_int_range() {
        let col = InMemoryColumn::from("a", Val::Int(10));
        let exp = InMemoryColumn::from("range(a)", Val::Int(10));
        assert_eq!(col.range().unwrap(), exp);
    }

    #[test]
    fn filter_col_eq_int() {
        let filter = Filter::new(Expr::Id(String::from("c")), Predicate::Equal, Expr::Int(1));
        let tbl = test_table("t", COL_LEN);
        let gate = filter.apply(&tbl).unwrap();
        let expected = vec![0];
        assert_eq!(gate, expected);
    }

    #[test]
    fn filter_col_lt_int() {
        let filter = Filter::new(Expr::Id(String::from("c")), Predicate::Less, Expr::Int(5));
        let tbl = test_table("t", COL_LEN);
        let gate = filter.apply(&tbl).unwrap();
        let expected = vec![0, 1, 2, 3];
        assert_eq!(gate, expected);
    }

    #[test]
    fn filter_col_lteq_int() {
        let filter = Filter::new(
            Expr::Id(String::from("c")),
            Predicate::LessEqual,
            Expr::Int(5),
        );
        let tbl = test_table("t", COL_LEN);
        let gate = filter.apply(&tbl).unwrap();
        let expected = vec![0, 1, 2, 3, 4];
        assert_eq!(gate, expected);
    }

    #[test]
    fn filter_col_gt_int() {
        let filter = Filter::new(
            Expr::Id(String::from("c")),
            Predicate::Greater,
            Expr::Int(5),
        );
        let tbl = test_table("t", COL_LEN);
        let gate = filter.apply(&tbl).unwrap();
        let expected = vec![5, 6, 7, 8, 9];
        assert_eq!(gate, expected);
    }

    #[test]
    fn filter_col_gteq_int() {
        let filter = Filter::new(
            Expr::Id(String::from("c")),
            Predicate::GreaterEqual,
            Expr::Int(5),
        );
        let tbl = test_table("t", COL_LEN);
        let gate = filter.apply(&tbl).unwrap();
        let expected = vec![4, 5, 6, 7, 8, 9];
        assert_eq!(gate, expected);
    }

    #[test]
    fn string_concatenation() {
        let expr = Expr::BinFn(
            Box::new(str_expr("abc")),
            BinOp::Add,
            Box::new(str_expr("def")),
        );
        let table = test_table("t", 10);
        let col = expr.eval(&table).unwrap();

        assert_eq!(col.name, "\"abc\" + \"def\"");
        assert_eq!(col.val, Val::Str(String::from("abcdef")));
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

    const REPEAT_LEN: usize = 20;
    const SORT_LEN: usize = 10_000;

    #[bench]
    fn bench_keys_vec_sort(b: &mut Bencher) {
        let n = SORT_LEN;
        b.iter(|| {
            let mut vec = Vec::new();
            let mut set = HashSet::new();
            for _ in 0..5 {
                for i in 0..n {
                    let val = i as i32;
                    if set.insert(val) {                        
                        vec.push(val);
                    }
                }
            }
            vec.par_sort();
        });
    }

    #[bench]
    fn bench_keys_vec_bsearch_sort(b: &mut Bencher) {
        let n = SORT_LEN;
        b.iter(|| {
            let mut vec = Vec::new();
            for _ in 0..REPEAT_LEN {
                for i in 0..n {
                    let val = i as i32;
                    if let Err(pos) = vec.binary_search(&val) {
                        vec.insert(pos, val);
                    }
                }
            }
        });
    }

    #[bench]
    fn bench_keys_btree_sort(b: &mut Bencher) {
        let n = SORT_LEN;
        b.iter(|| {
            let mut map = BTreeMap::new();
            for _ in 0..REPEAT_LEN {
                for i in 0..n {
                    map.insert(i as i32, 1);
                }
            }
        });
    }

    #[bench]
    fn bench_keys_bheap_sort(b: &mut Bencher) {
        let n = SORT_LEN;
        b.iter(|| {
            let mut heap = BinaryHeap::new();
            for _ in 0..REPEAT_LEN {
                for i in 0..n {
                    heap.push(i);
                }
            }
        });
    }    

}
