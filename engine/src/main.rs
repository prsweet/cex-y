use std::io::{self, BufRead};

use crate::types::{EngineCommand, EngineDB, EngineEvent};

mod types;

fn main()
{
    let stdin = io::stdin();
    let mut reader = stdin.lock();
    let mut line = String::new();

    while reader.read_line(&mut line).unwrap() > 0 {
        let cmd: Result<EngineCommand, _> = serde_json::from_str(line.trim());
        match cmd {
            Ok(cmd) => {
                match cmd {
                    EngineCommand::CancelOrder { symbol, order_id } => {
                        println!("got command to cancel order");
                        
                    }
                    EngineCommand::PlaceOrder { symbol, user_id, side, order_type, price, quantity } => {
                        println!("got command to place order");
                    }
                    EngineCommand::GetOrderBook { symbol } => {
                        println!("got command to get orderbook");
                    }
                }
            }
            Err(e) => {
                let event = EngineEvent::Error { message: format!("JSON parse error {}", e) };
                println!("{}", serde_json::to_string(&event).unwrap());
            }
        }
        line.clear();
    }
}