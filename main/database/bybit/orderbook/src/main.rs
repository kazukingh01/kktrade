use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, BufRead};

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
        for (price, amount) in &self.asks {
            println!("{}: {}", price, amount);
        }
    }
}

fn main() -> std::io::Result<()> {
    // 読み込み対象のファイル名
    let file = File::open("2024-09-01_ETHUSDT_ob500.data")?;
    let reader = BufReader::new(file);

    let mut order_book = OrderBook::new();

    for line in reader.lines() {
        let line = line?;
        let update: OrderBookUpdate = serde_json::from_str(&line).expect("Invalid JSON format");

        match update.update_type.as_str() {
            "snapshot" => {
                println!("Applying snapshot...");
                order_book.apply_snapshot(&update.data);
            }
            "delta" => {
                println!("Applying delta...");
                order_book.apply_delta(&update.data);
            }
            _ => println!("Unknown update type"),
        }

        // 常に最新のオーダーブック状態を表示
        order_book.display();
    }

    Ok(())
}
