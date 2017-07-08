#![feature(exclusive_range_pattern)]
#![feature(box_patterns)]
#![feature(test)]

extern crate chrono;
extern crate tabwriter;
extern crate test;
extern crate rayon;

mod aggregators;
mod engine;
mod parser;
mod computation;
mod tables;
mod databases;

use std::collections::VecDeque;
use std::io::{self, Write};
use std::time::Instant;

use databases::{InMemoryDb};
use engine::Val;
use tables::*;

const CMD_HISTORY_LEN: usize = 10;

#[derive(Debug)]
struct CmdHistory {
    cmds: VecDeque<String>,
}

impl CmdHistory {
    fn new() -> Self {
        CmdHistory {
            cmds: VecDeque::with_capacity(CMD_HISTORY_LEN),
        }
    }

    fn push(&mut self, cmd: String) {
        if self.cmds.len() == CMD_HISTORY_LEN {
            self.cmds.pop_front();
        }
        self.cmds.push_back(cmd);
    }
}

fn test_db(n: usize) -> InMemoryDb {
    let c1 = InMemoryColumn::from("a", Val::IntVec(vec![1; n]));
    let c2 = InMemoryColumn::from("b", Val::IntVec(vec![1; n]));
    let c3_val = (0..n).map(|x| x as i32).collect();
    let c3 = InMemoryColumn::from("c", Val::IntVec(c3_val));
    let table = InMemoryTable::from("t", vec![c1, c2, c3]);
    InMemoryDb::from(vec![table])
}

fn start_repl() {
    let n = 10_000_000;
    let db = test_db(n);
    
    loop {
        let mut cmd = String::new();
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
                let dns = dt.as_secs() * 1_000_000 + dt.subsec_nanos() as u64;
                println!("query executed in {:?} ns", dns);
                println!("{}", table);
            }
            Err(err) => println!("error: {}", err),
        }
    }
}

fn main() {
    start_repl();
}
