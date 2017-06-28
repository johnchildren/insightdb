#![feature(exclusive_range_pattern)]
#![feature(box_patterns)]

mod engine;
mod parser;

use std::io::{self, Write};

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
    loop {
        print!("insightdb> ");
        io::stdout().flush().unwrap();
        let mut cmd = String::new();
        match io::stdin().read_line(&mut cmd) {
            Ok(n) => (),
            Err(err) => println!("error: {}", err),
        }
        //println!("{}", cmd);
        cmd.trim_right_matches("\r\n");
        cmd.trim_right_matches("\n");
        match db.exec(&cmd) {
            Ok(table) => println!("{}", table),
            Err(err) => println!("error: {}", err),
        }
    }
}

fn main() {
    start_repl();
}
