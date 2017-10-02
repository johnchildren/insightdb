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
extern crate toml;
#[macro_use]
extern crate serde_derive;

mod config;
mod database;

use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

fn read_config_str<P: AsRef<Path>>(path: P) -> io::Result<String> {
    let mut f = try!(File::open(path));
    let mut s = String::new();
    f.read_to_string(&mut s);
    Ok(s)
}

fn main() {
    println!("starting insightdb");

    println!("loading config");
    let s = read_config_str("config.toml").expect("cannot read config");
    println!("config={}", s);
}