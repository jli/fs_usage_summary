use std::collections::HashMap;
use std::fmt::Display;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::u64;

use anyhow::{bail, Context, Result};
use lazy_static::lazy_static;
use regex::Regex;
use structopt::StructOpt;

const PRINT_EVERY_N_SECS: f32 = 3.;
const PRINT_TOP_N: usize = 10;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(parse(from_os_str))]
    input: PathBuf,
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    // let reader = open_reader(opt.input);
    let reader: Box<dyn BufRead> = if opt.input.to_string_lossy() == "-" {
        Box::new(BufReader::new(std::io::stdin()))
    } else {
        Box::new(BufReader::new(File::open(opt.input).unwrap()))
    };
    process_input(reader)
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
    call_entries: HashMap<String, u64>,
    process_time: HashMap<String, f64>,
    process_entries: HashMap<String, u64>,
}

impl Summary {
    fn new() -> Summary {
        Summary {
            lines: 0,
            parse_fails: 0,
            call_time: HashMap::new(),
            call_entries: HashMap::new(),
            process_time: HashMap::new(),
            process_entries: HashMap::new(),
        }
    }

    fn add(&mut self, rec: &DiskIoRec) {
        *self.process_time.entry(rec.process.clone()).or_insert(0.) += rec.interval;
        *self.process_entries.entry(rec.process.clone()).or_insert(0) += 1;
        *self.call_time.entry(rec.call.clone()).or_insert(0.) += rec.interval;
        *self.call_entries.entry(rec.call.clone()).or_insert(0) += 1;
    }
}

impl Display for Summary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn fmt_top<K: Display, V: Display + PartialOrd>(hash: &HashMap<K, V>) -> String {
            fmt_pairs(&top_values(hash, PRINT_TOP_N))
        }
        write!(
            f,
            "\n=> lines (fails): {} ({})\n\
            => top calls (time):\n{}\
            => top calls (entries):\n{}\
            => top processes (time):\n{}\
            => top processes (entries):\n{}",
            self.lines,
            self.parse_fails,
            fmt_top(&self.call_time),
            fmt_top(&self.call_entries),
            fmt_top(&self.process_time),
            fmt_top(&self.process_entries),
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

fn fmt_pairs<K: Display, V: Display>(pairs: &Vec<(K, V)>) -> String {
    let mut res = String::new();
    for (k, v) in pairs {
        // commands are max 16 chars
        let entry = format!("  {:>16}: {:.1}\n", k, v);
        res.push_str(&entry);
    }
    res
}

fn process_input(reader: Box<dyn BufRead>) -> Result<()> {
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
                summary.parse_fails += 1;
                // TODO: how to catch this case specifically to ignore it..?
                let es = e.to_string();
                if es != "(errno)" {
                    println!("{:?}", e);
                }
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

lazy_static! {
    static ref LINE_RE: Regex = Regex::new(
        r"(\d{2}:\d{2}:\d{2}.\d+) +([^ ]+) .* B=0x([[:xdigit:]]+) .* ([.\d]+) W (.+)\.(\d+)$"
    )
    .unwrap();
    static ref ERRNO_RE: Regex = Regex::new(r" \[([ \d]+)\]").unwrap();
}

// TODO: way to make this more concise?
fn parse_line(s: &str) -> Result<DiskIoRec> {
    let cap = LINE_RE.captures(s);
    if cap.is_none() {
        if !ERRNO_RE.is_match(s) {
            bail!("unexpected parse, no bytes or errno: {}", s);
        }
        bail!("(errno)");
    }
    let cap = cap.context("regex match failed on line")?;
    Ok(DiskIoRec {
        timestamp: cap.get(1).context("timestamp")?.as_str().into(),
        call: cap.get(2).context("call")?.as_str().to_string(),
        bytes: u64::from_str_radix(cap.get(3).context("bytes")?.as_str(), 16)?,
        interval: cap.get(4).context("interval")?.as_str().parse()?,
        process: cap.get(5).context("process")?.as_str().into(),
        pid: cap.get(6).context("pid")?.as_str().parse()?,
    })
}
