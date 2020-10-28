use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use structopt::StructOpt;

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
    let f = File::open(path)?;
    let reader = BufReader::new(f);
    for line in reader.lines() {
        let line = line?;
        println!("line {}", line);
        let rec = parse_line(line);
        println!("rec {:?}", rec);
    }
    Ok(())
}

// TODO: parse line
fn parse_line(s: String) -> DiskIoRec {
    DiskIoRec {
        timestamp: "1".to_string(),
        call: "op".to_string(),
        bytes: 1,
        interval: 0.1,
        process: "op".to_string(),
    }
}
