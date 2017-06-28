#![feature(exclusive_range_pattern)]
#![feature(box_patterns)]

extern crate chrono;
extern crate tabwriter;

mod engine;
mod parser;

use chrono::prelude::*;

use std::io::{self, Write};
use std::time::Instant;

use engine::{Column, Database, Table, Val};

fn test_db() -> Database {
    let n = 5;
    let c1 = Column::from("a", Val::IntVec(vec![1; n]));
    let c2 = Column::from("b", Val::IntVec(vec![1; n]));
    let c3 = Column::from("c", Val::IntVec(vec![1,2,3,4,5]));
    let table = Table::from("t", vec![c1, c2, c3]);
    Database::from(vec![table])
}

fn start_repl() {
    let db = test_db();
    let mut cmd = String::new();
    loop {
        print!("insightdb> ");
        io::stdout().flush().unwrap();       
        match io::stdin().read_line(&mut cmd) {
            Ok(_) => (),
            Err(err) => println!("error: {}", err),
        }
        //println!("{}", cmd);
        cmd.trim_right_matches("\r\n");
        cmd.trim_right_matches("\n");
        let now = Instant::now();
        match db.exec(&cmd) {
            Ok(table) => {
                let dt = now.elapsed();
                println!("query executed in {} nanoseconds", dt.as_secs() * 1_000_000_000 + dt.subsec_nanos() as u64);
                println!("{}", table);
            }
            Err(err) => println!("error: {}", err),
        }
        cmd.clear();
    }
}

fn main() {
    start_repl();
}
