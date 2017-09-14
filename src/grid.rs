use std::net::IpAddr;
use std::result;

use engine::Expr;

struct QueryData;

pub struct GridQueryCmd {
    select: Vec<GridExpr>,
    from: String,
}

pub struct Data;

type Result<T> = result::Result<T, String>;

pub trait GridQuery {
    fn exec(&self, cmd: &GridQueryCmd) -> Result<Data>;
}

enum GridExpr {
    Avg(Expr, Expr),
    Reg(Expr),
}


impl GridQuery for Grid {
    fn exec(&self, cmd: &GridQueryCmd) -> Result<Data> {
        unimplemented!()
    }
}

pub struct Params {
    table: String,
    column: String,
}

pub trait Sum<T> {
    fn sum(&self, params: &Params) -> Result<T>;
}

pub trait Max<T> {
    fn max(&self, params: &Params) -> Result<T>;
}

pub trait Min<T> {
    fn min(&self,  params: &Params) -> Result<T>;
}

pub trait Product<T> {
    fn product(&self, params: &Params) -> Result<T>;
}

pub trait Average {
    fn average(&self, params: &Params) -> Result<f64>;
}

pub trait Count {
    fn count(&self, params: &Params) -> Result<usize>;
}

pub struct Grid {
    remote_dbs: Vec<RemoteDb>,
}

impl Grid {
    fn from(remote_dbs: Vec<RemoteDb>) -> Self {
        Self{ remote_dbs } 
    }
}

impl Sum<i32> for Grid {
    fn sum(&self, params: &Params) -> Result<i32> {
        let mut sum = 0;
        for db in &self.remote_dbs {
            match db.sum(params) {
                Ok(val) => sum += val,
                Err(err) => return Err(err),
            }
        }
        Ok(sum)
    }
}

impl Max<i32> for Grid {
    fn max(&self, params: &Params) -> Result<i32> {
        let mut max = 0;
        for db in &self.remote_dbs {
            match db.max(params) {
                Ok(val) if val > max => max = val,
                _ => continue,
                Err(err) => return Err(err),
            }
        }
        Ok(max)
    }
}

impl Min<i32> for Grid {
    fn min(&self, params: &Params) -> Result<i32> {
        let mut min = 0;
        for db in &self.remote_dbs {
            match db.min(params) {
                Ok(val) if val < min => min = val,
                Ok(_) => continue,
                Err(err) => return Err(err),
            }
        }
        Ok(min)
    }
}

impl Product<i32> for Grid {
    fn product(&self, params: &Params) -> Result<i32> {
        let mut product = 1;
        for db in &self.remote_dbs {
            match db.product(params) {
                Ok(val) => product *= val,
                Err(err) => return Err(err),
            }
        }
        Ok(product)
    }
}

impl Average for Grid {
    fn average(&self, params: &Params) -> Result<f64> {
        let sum = match self.sum(params) {
            Ok(val) => val as f64,
            Err(err) => return Err(err),
        };
        let count = match self.count(params) {
            Ok(n) => n as f64,
            Err(err) => return Err(err),
        };
        Ok(sum / count)
    }
}

impl Count for Grid {
    fn count(&self, params: &Params) -> Result<usize> {
        let mut count = 0;
        for db in &self.remote_dbs {
            match db.count(params) {
                Ok(n) => count += n,
                Err(err) => return Err(err),
            }
        }
        Ok(count)
    }
}

pub struct RemoteDb {
    host: IpAddr,
}

impl RemoteDb {
    pub fn new(host: IpAddr) -> Self {
        Self { host }
    }
}

impl Sum<i32> for RemoteDb {
    fn sum(&self, params: &Params) -> Result<i32> {
        unimplemented!();
    } 
}

impl Max<i32> for RemoteDb {
    fn max(&self, params: &Params) -> Result<i32> {
        unimplemented!()
    }
}

impl Min<i32> for RemoteDb {
    fn min(&self, params: &Params) -> Result<i32> {
        unimplemented!()
    }
}

impl Product<i32> for RemoteDb {
    fn product(&self, params: &Params) -> Result<i32> {
        unimplemented!()
    }
}

impl Count for RemoteDb {
    fn count(&self, params: &Params) -> Result<usize> {
        unimplemented!()
    }
}

