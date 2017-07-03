use std::cmp;
use std::iter::Sum;
use std::ops::{Add, Div, Mul, Sub};

use rayon::prelude::*;

#[inline]
pub fn vec_add(a: &[i32], b: &[i32]) -> Vec<i32> {
    a.par_iter()
        .zip(b.par_iter())
        .map(|(x, y)| *x + *y)
        .collect()
}

#[inline]
pub fn vec_add_mut(a: &mut [i32], b: &[i32]) {
    a.par_iter_mut().zip(b.par_iter()).for_each(
        |(x, y)| *x *= *y,
    )
}

#[inline]
pub fn vec_sub(a: &[i32], b: &[i32]) -> Vec<i32> {
    a.par_iter()
        .zip(b.par_iter())
        .map(|(x, y)| *x - *y)
        .collect()
}

#[inline]
pub fn vec_mul<T: Mul<Output = T> + Copy>(a: &[T], b: &[T]) -> Vec<T> {
    a.iter().zip(b.iter()).map(|(x, y)| *x * *y).collect()
}

#[inline]
pub fn veci32_i32mul(a: &[i32], b: i32) -> Vec<i32> {
    a.par_iter().map(|x| *x * b).collect()
}

#[inline]
pub fn vec_div(a: &[i32], b: &[i32]) -> Vec<i32> {
    a.par_iter()
        .zip(b.par_iter())
        .map(|(x, y)| *x / *y)
        .collect()
}

#[inline]
pub fn vec_min(v: &[i32]) -> Option<i32> {
    v.par_iter().min().map(|x| *x)
}

#[inline]
pub fn vec_max(v: &[i32]) -> Option<i32> {
    v.par_iter().max().map(|x| *x)
}

#[inline]
pub fn vec_max_iter(v: &[i32]) -> Option<i32> {
    let mut it = v.iter();
    it.next().map(|mut sel| {
        println!("sel={:?}", sel);
        for val in it {
            println!("val={:?}", val);
            if val > sel {
                sel = val
            }
        }
        *sel
    })
}

#[inline]
pub fn vec_maxs<T: Ord + Copy>(v: &[T]) -> Vec<T> {
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
pub fn vec_products<T: Ord + Copy + Mul<Output = T>>(v: &[T]) -> Vec<T> {
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
pub fn vec_int_range(v: &[i32]) -> Vec<i32> {
    assert!(!v.is_empty());
    let mut min = v[0];
    let mut max = min;
    for val in &v[1..] {
        max = cmp::max(*val, max);
        min = cmp::min(*val, min);
    }
    let n = (max - min) as usize + 1;
    let mut range = Vec::with_capacity(n);
    for i in (0..n).map(|i| i as i32) {
        range.push(min + i)
    }
    range
}

#[inline]
pub fn vec_sums(v: &[i32]) -> Vec<i32> {
    let mut scan = Vec::with_capacity(v.len());
    let mut acc = 0;
    for val in v {
        acc += *val;
        scan.push(acc);
    }
    scan
}

#[inline]
pub fn vec_sum<'a, T>(v: &'a [T]) -> T
where
    T: Send + Sum<&'a T> + Sum + Sync + Copy,
{
    v.par_iter().sum()
}

#[inline]
pub fn vec_mins<T: Ord + Copy>(v: &[T]) -> Vec<T> {
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
pub fn ranged_vec(start: usize, end: usize) -> Vec<i32> {
    (start..end).map(|x| x as i32).collect()
}

#[inline]
pub fn vec_scalar_sub<T: Sub<Output = T> + Copy>(a: &[T], b: T) -> Vec<T> {
    a.iter().map(|x| *x - b).collect()
}

#[inline]
pub fn vec_scalar_add<T: Add<Output = T> + Copy>(a: &[T], b: T) -> Vec<T> {
    a.iter().map(|x| *x + b).collect()
}

#[inline]
pub fn vec_scalar_mul<T: Mul<Output = T> + Copy>(a: &[T], b: T) -> Vec<T> {
    a.iter().map(|x| *x * b).collect()
}

#[inline]
pub fn vec_scalar_div<T: Div<Output = T> + Copy>(a: &[T], b: T) -> Vec<T> {
    a.iter().map(|x| *x / b).collect()
}

#[inline]
pub fn str_add(a: &str, b: &str) -> String {
    a.to_string() + b
}

#[inline]
pub fn strs_add(a: &[String], b: &[String]) -> Vec<String> {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| x.to_string() + y)
        .collect()
}