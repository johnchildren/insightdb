use std::ops::AddAssign;

pub trait Aggregate<T> {
    fn push(&mut self, val: T);
    fn aggregate(&self) -> &T;
}

#[derive(Clone)]
pub struct MaxAggregator<T> {
    max: T,
}

impl<T> MaxAggregator<T> {
    pub fn new(val: T) -> Self {
        Self { max: val }
    }
}

impl<T:PartialOrd + 'static + Clone> Aggregate<T> for MaxAggregator<T> {
    fn push(&mut self, val: T)  {
        if val > self.max {
            self.max = val;
        }
    }

    fn aggregate(&self) -> &T {
        &self.max
    }
}

#[derive(Clone)]
pub struct MinAggregator<T> {
    min: T,
}

impl<T: Clone> MinAggregator<T> {
    pub fn new(val: T) -> Self {
        Self {
            min: val,
        }
    }
}

impl<T:PartialOrd + Clone + 'static> Aggregate<T> for MinAggregator<T> {
    fn push(&mut self, val: T) {
        if val < self.min {
            self.min = val;
        }
    }

    fn aggregate(&self) -> &T {
        &self.min
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct SumAggregator<T> {
    sum: T,
}

impl<T:Default> SumAggregator<T> {
    pub fn new() -> SumAggregator<T> {
        SumAggregator::from(T::default())
    }

    pub fn from(val: T) -> SumAggregator<T> {
        SumAggregator { sum: val }
    }
}

impl<T:AddAssign + Clone + 'static> Aggregate<T> for SumAggregator<T> {
    fn push(&mut self, val: T) {
        self.sum += val;
    }

    fn aggregate(&self) -> &T {
        &self.sum
    }
    
    /*
    fn box_clone(&self) -> Box<Aggregate<T>> {
        Box::new(self.clone())
    }
    */
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
        let mut agg = MaxAggregator::new(0);
        let n = 10;
        for val in 0..n {
            agg.push(val);
        }
        for val in 0..n {
            agg.push(n-1-val);
        }
        assert_eq!(agg.aggregate().clone(), 9);
    }

    #[test]
    fn min_aggregate() {        
        let mut agg = MinAggregator::new(0);
        let n = 10;
        for val in 0..n {
            agg.push(val);
        }
        for val in 0..n {
            agg.push(n-val);
        }        
        assert_eq!(agg.aggregate().clone(), 0);
    }

    #[test]
    fn sum_aggregate() {        
        let mut agg = SumAggregator::new();
        for _ in 0..10 {
            agg.push(1);
        }
        assert_eq!(agg.aggregate().clone(), 10);
    }    
}