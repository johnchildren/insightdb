#![feature(exclusive_range_pattern)]
#![feature(box_patterns)]
#![feature(test)]

extern crate chrono;
extern crate tabwriter;
extern crate test;
extern crate rayon;
extern crate bytes;
extern crate futures;
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_service;

pub mod engine;
pub mod aggregators;
pub mod computation;
pub mod databases;
pub mod parser;
pub mod tables;
pub mod grid;
pub mod server;