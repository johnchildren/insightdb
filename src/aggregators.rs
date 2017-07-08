use std::cmp::Ordering;
use std::ops::{Add, AddAssign};

pub trait Aggregate<T> {
    fn push(&mut self, val: T);
    fn aggregate(&self) -> &T;
}

pub struct MaxAggregate<T> {
    max: T,
}

impl<T> MaxAggregate<T> {
    fn new(val: T) -> Self {
        MaxAggregate { max: val }
    }
}

impl<T:PartialOrd> Aggregate<T> for MaxAggregate<T> {
    fn push(&mut self, val: T)  {
        if val > self.max {
            self.max = val;
        }
    }

    fn aggregate(&self) -> &T {
        &self.max
    }
}

pub struct MinAggregate<T> {
    min: T,
}

impl<T> MinAggregate<T> {
    fn new(val: T) -> Self {
        MinAggregate {
            min: val,
        }
    }
}

impl<T:PartialOrd> Aggregate<T> for MinAggregate<T> {
    fn push(&mut self, val: T) {
        if val < self.min {
            self.min = val;
        }
    }

    fn aggregate(&self) -> &T {
        &self.min
    }
}

pub struct SumAggregate<T> {
    sum: T,
}

impl<T: AddAssign + Default> SumAggregate<T> {
    pub fn new() -> Self {
        SumAggregate {
            sum: T::default(),
        }
    }
    pub fn from(val: T) -> Self {
        SumAggregate {
            sum: val,
        }
    }
}

impl<T:AddAssign> Aggregate<T> for SumAggregate<T> {
    fn push(&mut self, val: T) {
        self.sum += val;
    }

    fn aggregate(&self) -> &T {
        &self.sum
    }
}

/*
pub struct Collect<T> {
    data: Vec<T>,
}

impl<T> Collect<T> {
    fn new() -> Self {
        Collect {
            data: Vec::new(),
        }
    }
}

impl<T> Aggregate<T> for Collect<T> {
    fn push(&mut self, val: T)  {
        self.data.push(val)
    }

    fn aggregate(self) -> Vec<T> {
        self.data
    } 
}

pub struct Chain<T> {
    inner: Box<Aggregate<T>>,
    outer: Box<Aggregate<T>>,
}
*/

#[cfg(test)]
mod tests {
    use super::*;
    use test::Bencher;

    const COL_LEN: usize = 10;

    #[test]
    fn max_aggregate() {        
        let mut agg = MaxAggregate::new(0);
        for val in 0..10 {
            agg.push(val);
        }
        assert_eq!(agg.aggregate(), 9);
    }

    #[test]
    fn min_aggregate() {        
        let mut agg = MinAggregate::new(0);
        for val in 0..10 {
            agg.push(val);
        }
        assert_eq!(agg.aggregate(), 0);
    }
}