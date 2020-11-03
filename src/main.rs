use std::fs::File;
use std::u64;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use structopt::StructOpt;

use lazy_static::lazy_static;
use regex::Regex;

type Err = Box<dyn std::error::Error>;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(parse(from_os_str))]
    input: PathBuf,
}

#[derive(Debug)]
struct DiskIoRec {
    timestamp: String,
    call: String,
    bytes: u64,
    interval: f64,
    process: String,
    pid: u64,
}

fn main() {
    let opt = Opt::from_args();
    if let Err(e) = process_file(opt.input) {
        println!("ERROR: {:?}", e);
        panic!("fail");
    }
}

// TODO: collect summary info
fn process_file(path: PathBuf) -> Result<(), Err> {
    println!("handling path: {:?}", path);
    let mut fails = 0;
    let f = File::open(path)?;
    let reader = BufReader::new(f);
    for line in reader.lines() {
        let line = line?;
        // println!("line {}", line);
        let rec = parse_line(&line);
        match rec {
            Ok(rec) => println!("rec {:?}", rec),
            Err(e) => {
                println!("error parsing line: {:?}\n => {}", e, line);
                fails += 1;
            }
        }
    }
    println!("failed parses: {}", fails);
    Ok(())
}

// TODO: make this less terrible?
fn parse_line(s: &str) -> Result<DiskIoRec, Err> {
    lazy_static! {
        static ref LINE_RE: Regex = Regex::new(
                r"(\d{2}:\d{2}:\d{2}.\d+) +([^ ]+) .* B=0x([[:xdigit:]]+) .* ([.\d]+) W (.+)\.(\d+)$"
            ).unwrap();
        static ref ERRNO_RE: Regex = Regex::new(r" \[([ \d]+)\]").unwrap();
    }
    let cap = LINE_RE.captures(s);
    if cap.is_none() {
        if !ERRNO_RE.is_match(s) {
            return Err("unexpected parse, no bytes or errno".into());
        }
        return Err("errno case, ignored.".into())
    }
    let cap = cap.ok_or("failed to match")?;
    Ok (DiskIoRec {
        timestamp: cap.get(1).ok_or("timestamp")?.as_str().to_string(),
        call: cap.get(2).ok_or("call")?.as_str().to_string(),
        bytes: u64::from_str_radix(cap.get(3).ok_or("bytes")?.as_str(), 16)?,
        interval: cap.get(4).ok_or("interval")?.as_str().parse()?,
        process: cap.get(5).ok_or("process")?.as_str().to_string(),
        pid: cap.get(6).ok_or("pid")?.as_str().parse()?,
    })
}
