#![feature(exclusive_range_pattern)]
#![feature(box_patterns)]
#![feature(test)]

extern crate bytes;
extern crate chrono;
extern crate clap;
extern crate csv;
extern crate futures;
extern crate rayon;
extern crate serde;
extern crate serde_json;
extern crate tabwriter;
extern crate test;
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_service;
extern crate toml;

#[macro_use]
extern crate serde_derive;

mod config;
mod database;

use std::io::{self, Read};

use clap::{Arg, App, SubCommand};
use database::Database;

fn start_server(config: &str) {
    let db = match Database::from_path(config) {
        Ok(db) => db,
        Err(err) => {
            println!("cannot create db: {}", err);
            return;
        }
    };
    db.start();
}

fn main() {
    println!("starting insightdb");
    let matches = App::new("My Super Program")
        .version("1.0")
        .author("Kevin K. <kbknapp@gmail.com>")
        .about("Does awesome things")
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file")
                .required(false)
                .takes_value(true),
        )
        .get_matches();
    let config = matches.value_of("config").unwrap_or("config.json");
    start_server(config);
}
