use std::net::IpAddr;
use std::result;

type Result<T> = result::Result<T, String>;

pub trait Sum<T> {
    fn sum(&self) -> Result<T>;
}

pub trait Max<T> {
    fn max(&self) -> Result<T>;
}

pub trait Min<T> {
    fn min(&self) -> Result<T>;
}

pub trait Product<T> {
    fn product(&self) -> Result<T>;
}

pub trait Average {
    fn average(&self) -> Result<f64>;
}

pub trait Count {
    fn count(&self) -> Result<usize>;
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
    fn sum(&self) -> Result<i32> {
        let mut sum = 0;
        for db in &self.remote_dbs {
            match db.sum() {
                Ok(val) => sum += val,
                Err(err) => return Err(err),
            }
        }
        Ok(sum)
    }
}

impl Max<i32> for Grid {
    fn max(&self) -> Result<i32> {
        let mut max = 0;
        for db in &self.remote_dbs {
            match db.max() {
                Ok(val) if val > max => max = val,
                _ => continue,
                Err(err) => return Err(err),
            }
        }
        Ok(max)
    }
}

impl Min<i32> for Grid {
    fn min(&self) -> Result<i32> {
        let mut min = 0;
        for db in &self.remote_dbs {
            match db.min() {
                Ok(val) if val < min => min = val,
                Ok(_) => continue,
                Err(err) => return Err(err),
            }
        }
        Ok(min)
    }
}

impl Product<i32> for Grid {
    fn product(&self) -> Result<i32> {
        let mut product = 1;
        for db in &self.remote_dbs {
            match db.product() {
                Ok(val) => product *= val,
                Err(err) => return Err(err),
            }
        }
        Ok(product)
    }
}

impl Average for Grid {
    fn average(&self) -> Result<f64> {
        let sum = match self.sum() {
            Ok(val) => val as f64,
            Err(err) => return Err(err),
        };
        let count = match self.count() {
            Ok(n) => n as f64,
            Err(err) => return Err(err),
        };
        Ok(sum / count)
    }
}

impl Count for Grid {
    fn count(&self) -> Result<usize> {
        let mut count = 0;
        for db in &self.remote_dbs {
            match db.count() {
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
    fn sum(&self) -> Result<i32> {
        unimplemented!();
    } 
}

impl Max<i32> for RemoteDb {
    fn max(&self) -> Result<i32> {
        unimplemented!()
    }
}

impl Min<i32> for RemoteDb {
    fn min(&self) -> Result<i32> {
        unimplemented!()
    }
}

impl Product<i32> for RemoteDb {
    fn product(&self) -> Result<i32> {
        unimplemented!()
    }
}

impl Count for RemoteDb {
    fn count(&self) -> Result<usize> {
        unimplemented!()
    }
}

