use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::u64;

use lazy_static::lazy_static;
use regex::Regex;
use structopt::StructOpt;

type Err = Box<dyn std::error::Error>;

const PRINT_EVERY_N_SECS: f32 = 3.;
const PRINT_TOP_N: usize = 10;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(parse(from_os_str))]
    input: PathBuf,
}

fn main() {
    let opt = Opt::from_args();
    // let reader = open_reader(opt.input);
    let reader: Box<dyn BufRead> = if opt.input.to_string_lossy() == "-" {
        Box::new(BufReader::new(std::io::stdin()))
    } else {
        Box::new(BufReader::new(File::open(opt.input).unwrap()))
    };
    if let Err(e) = process_input(reader) {
        println!("ERROR: {:?}", e);
        panic!("fail");
    }
}

// TODO: fails with 'creates a temporary which is freed while still in use' on stdin()
// fn open_reader(input: PathBuf) -> Box<dyn BufRead> {
//     if input.to_string_lossy() == "-" {
//         Box::new(BufReader::new(std::io::stdin().lock()))
//     } else {
//         Box::new(BufReader::new(File::open(input).unwrap()))
//     }
// }

#[derive(Debug)]
struct DiskIoRec {
    timestamp: String,
    call: String,
    bytes: u64,
    interval: f64,
    process: String,
    pid: u64,
}

#[derive(Debug)]
struct Summary {
    lines: u64,
    parse_fails: u64,
    call_time: HashMap<String, f64>,
    process_time: HashMap<String, f64>,
}

impl Summary {
    fn new() -> Summary {
        Summary {
            lines: 0,
            parse_fails: 0,
            call_time: HashMap::new(),
            process_time: HashMap::new(),
        }
    }

    fn add(&mut self, rec: &DiskIoRec) {
        *self.process_time.entry(rec.process.clone()).or_insert(0.) += rec.interval;
        *self.call_time.entry(rec.call.clone()).or_insert(0.) += rec.interval;
    }
}

impl std::fmt::Display for Summary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let top_calls = top_values(&self.call_time, PRINT_TOP_N);
        let top_procs = top_values(&self.process_time, PRINT_TOP_N);
        write!(
            f,
            "lines (fails): {} ({})\ntop calls:\n{}top processes:\n{}\n",
            self.lines,
            self.parse_fails,
            fmt_pairs(&top_calls),
            fmt_pairs(&top_procs)
        )
    }
}

// TODO: um store in a better data structure to avoid this?
fn top_values<K, V: PartialOrd>(hash: &HashMap<K, V>, n: usize) -> Vec<(&K, &V)> {
    let mut top = vec![];
    let mut vals: Vec<(usize, &V)> = hash.values().enumerate().collect();
    vals.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap());
    let keys: Vec<&K> = hash.keys().collect();
    for &(i, val) in vals.iter().take(n) {
        top.push((keys[i], val));
    }
    top
}

fn fmt_pairs<K: std::fmt::Display, V: std::fmt::Display>(pairs: &Vec<(K, V)>) -> String {
    let mut res = String::new();
    for (k, v) in pairs {
        let entry = format!("  {}: {}\n", k, v);
        res.push_str(&entry);
    }
    res
}

fn process_input(reader: Box<dyn BufRead>) -> Result<(), Err> {
    let mut summary = Summary::new();
    let mut last_print = std::time::UNIX_EPOCH;
    for line in reader.lines() {
        let line = line?;
        summary.lines += 1;
        let rec = parse_line(&line);
        match rec {
            // Ok(rec) => { println!("{:?}", rec); summary.add(&rec) },
            Ok(rec) => summary.add(&rec),
            Err(e) => {
                println!("{:?}", e);
                summary.parse_fails += 1;
            }
        }
        let now = std::time::SystemTime::now();
        match now.duration_since(last_print) {
            Err(_) => last_print = now,
            Ok(n) => {
                if n.as_secs_f32() >= PRINT_EVERY_N_SECS {
                    println!("\n{}", summary);
                    last_print = now;
                }
            }
        }
    }
    // reached end of input, print final summary
    println!("\n{}", summary);
    Ok(())
}

// TODO: make this less terrible?
fn parse_line(s: &str) -> Result<DiskIoRec, Err> {
    lazy_static! {
        static ref LINE_RE: Regex = Regex::new(
            r"(\d{2}:\d{2}:\d{2}.\d+) +([^ ]+) .* B=0x([[:xdigit:]]+) .* ([.\d]+) W (.+)\.(\d+)$"
        )
        .unwrap();
        static ref ERRNO_RE: Regex = Regex::new(r" \[([ \d]+)\]").unwrap();
    }
    let cap = LINE_RE.captures(s);
    if cap.is_none() {
        if !ERRNO_RE.is_match(s) {
            return Err(format!("unexpected parse, no bytes or errno: {}", s).into());
        }
        return Err("(errno)".into());
    }
    let cap = cap.ok_or("failed to match")?;
    Ok(DiskIoRec {
        timestamp: cap.get(1).ok_or("timestamp")?.as_str().to_string(),
        call: cap.get(2).ok_or("call")?.as_str().to_string(),
        bytes: u64::from_str_radix(cap.get(3).ok_or("bytes")?.as_str(), 16)?,
        interval: cap.get(4).ok_or("interval")?.as_str().parse()?,
        process: cap.get(5).ok_or("process")?.as_str().to_string(),
        pid: cap.get(6).ok_or("pid")?.as_str().parse()?,
    })
}
