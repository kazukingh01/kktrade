use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::io::{BufReader, BufRead, Write};
use chrono::{Utc, TimeZone};

#[derive(Debug, Deserialize, Serialize)]
struct OrderBookUpdate {
    topic: String,
    #[serde(rename = "type")]
    update_type: String,
    ts: u64,
    data: OrderBookData,
}

#[derive(Debug, Deserialize, Serialize)]
struct OrderBookData {
    s: String,
    b: Vec<(String, String)>, // (price, amount)
    a: Vec<(String, String)>, // (price, amount)
    u: u64,
    seq: u64,
}

#[derive(Debug, Clone)]
struct OrderBook {
    bids: BTreeMap<String, String>,
    asks: BTreeMap<String, String>,
}

impl OrderBook {
    fn new() -> Self {
        OrderBook {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    fn apply_snapshot(&mut self, data: &OrderBookData) {
        self.bids.clear();
        self.asks.clear();
        for (price, amount) in &data.b {
            if amount != "0" {
                self.bids.insert(price.clone(), amount.clone());
            }
        }
        for (price, amount) in &data.a {
            if amount != "0" {
                self.asks.insert(price.clone(), amount.clone());
            }
        }
    }

    fn apply_delta(&mut self, data: &OrderBookData) {
        // Update bids
        for (price, amount) in &data.b {
            if amount == "0" {
                self.bids.remove(price);
            } else {
                self.bids.insert(price.clone(), amount.clone());
            }
        }

        // Update asks
        for (price, amount) in &data.a {
            if amount == "0" {
                self.asks.remove(price);
            } else {
                self.asks.insert(price.clone(), amount.clone());
            }
        }
    }

    fn display(&self) {
        println!("Bids:");
        for (price, amount) in self.bids.iter().rev() {
            println!("{}: {}", price, amount);
        }
        println!("Asks:");
        for (price, amount) in self.asks.iter().rev() {
            println!("{}: {}", price, amount);
        }
    }

    fn to_csv(&self, ts: i64) -> String {
        let mut csv_str = String::new();
        for (price, amount) in self.bids.iter().rev() {
            csv_str.push_str(&format!("{},1,{},{}\n", ts, price, amount));
        }
        for (price, amount) in self.asks.iter().rev() {
            csv_str.push_str(&format!("{},0,{},{}\n", ts, price, amount));
        }
        csv_str
    }
}

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 || args.len() > 3 {
        eprintln!("Usage: {} <input_file>", args[0]);
        std::process::exit(1);
    }

    let mut output = "tmp.csv";
    if args.len() == 3 {
        output = &args[2];
    }

    let file = File::open(&args[1])?;
    let reader = BufReader::new(file);

    let mut order_book = OrderBook::new();
    let mut csv_str = String::new();
    csv_str.push_str("unixtime,side,price,size\n");
    let mut bool_tmp = true;

    for line in reader.lines() {
        let line = line?;
        let update: OrderBookUpdate = serde_json::from_str(&line).expect("Invalid JSON format");

        match update.update_type.as_str() {
            "snapshot" => {
                println!("Applying snapshot...");
                order_book.apply_snapshot(&update.data);
            }
            "delta" => {
                // println!("Applying delta...");
                order_book.apply_delta(&update.data);
            }
            _ => println!("Unknown update type"),
        }

        let unixtime: i64 = (update.ts / 1000).try_into().expect("Value out of range for i64");
        if (unixtime % 10) == 0 {
            if bool_tmp {
                csv_str += &order_book.to_csv(unixtime);
                bool_tmp = false;
                let datetime_utc = Utc.timestamp_opt(unixtime, 0)
                                        .single()
                                        .expect("Invalid timestamp");
                println!("unixtime: {}, {}, {}", unixtime, unixtime % 10, datetime_utc);    
            }
        } else {
            bool_tmp = true;
        }

    }
    // write to CSV file.
    let mut file = File::create(output)?;
    file.write_all(csv_str.as_bytes())?;

    Ok(())
}
