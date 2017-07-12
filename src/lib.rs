#![feature(exclusive_range_pattern)]
#![feature(box_patterns)]
#![feature(test)]

extern crate chrono;
extern crate tabwriter;
extern crate test;
extern crate rayon;

pub mod engine;
pub mod aggregators;
pub mod computation;
pub mod databases;
pub mod parser;
pub mod tables;