use std::ops::AddAssign;

pub trait Aggregate<T> {
    fn push(&mut self, val: T);
    fn aggregate(&self) -> &T;
    fn box_clone(&self) -> Box<Aggregate<T>>;
}

#[derive(Clone)]
pub struct MaxAggregate<T> {
    max: T,
}

impl<T> MaxAggregate<T> {
    pub fn new(val: T) -> Self {
        MaxAggregate { max: val }
    }
}

impl<T:PartialOrd + 'static + Clone> Aggregate<T> for MaxAggregate<T> {
    fn push(&mut self, val: T)  {
        if val > self.max {
            self.max = val;
        }
    }

    fn aggregate(&self) -> &T {
        &self.max
    }

    fn box_clone(&self) -> Box<Aggregate<T>> {
        Box::new(self.clone())
    }
}

#[derive(Clone)]
pub struct MinAggregate<T> {
    min: T,
}

impl<T: Clone> MinAggregate<T> {
    pub fn new(val: T) -> Self {
        MinAggregate {
            min: val,
        }
    }
}

impl<T:PartialOrd + Clone + 'static> Aggregate<T> for MinAggregate<T> {
    fn push(&mut self, val: T) {
        if val < self.min {
            self.min = val;
        }
    }

    fn aggregate(&self) -> &T {
        &self.min
    }

    fn box_clone(&self) -> Box<Aggregate<T>> {
        Box::new(self.clone())
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct SumAggregate<T> {
    sum: T,
}

impl<T:Default> SumAggregate<T> {
    pub fn new() -> Self {
        SumAggregate::from(T::default())
    }

    pub fn from(val: T) -> Self {
        SumAggregate { sum: val }
    }
}

impl<T:AddAssign + Clone + 'static> Aggregate<T> for SumAggregate<T> {
    fn push(&mut self, val: T) {
        self.sum += val;
    }

    fn aggregate(&self) -> &T {
        &self.sum
    }
    
    fn box_clone(&self) -> Box<Aggregate<T>> {
        Box::new(self.clone())
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

    #[test]
    fn max_aggregate() {        
        let mut agg = MaxAggregate::new(0);
        for val in 0..10 {
            agg.push(val);
        }
        assert_eq!(agg.aggregate().clone(), 9);
    }

    #[test]
    fn min_aggregate() {        
        let mut agg = MinAggregate::new(0);
        for val in 0..10 {
            agg.push(val);
        }
        assert_eq!(agg.aggregate().clone(), 0);
    }

    #[test]
    fn sum_aggregate() {        
        let mut agg = SumAggregate::new();
        for _ in 0..10 {
            agg.push(1);
        }
        assert_eq!(agg.aggregate().clone(), 10);
    }    
}