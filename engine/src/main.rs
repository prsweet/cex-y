use std::collections::HashMap;

use chrono::Utc;
use krafka::consumer::{AutoOffsetReset, Consumer};
use tokio::sync::mpsc::{self, Receiver};
use ulid::Ulid;

use crate::types::*;

mod types;

#[tokio::main]
async fn main() {
    let consumer = Consumer::builder()
        .bootstrap_servers("localhost:9092")
        .enable_auto_commit(true)
        .group_id("cex-y-engine")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();

    consumer.subscribe(&["orders"]).await.unwrap();

    let mut actors: HashMap<String, mpsc::Sender<EngineCommand>> = HashMap::new();

    loop {
        match consumer.recv().await {
            Ok(received) => {
                if let Ok(cmd) =
                    serde_json::from_str::<EngineCommand>(&received.value_str().unwrap())
                {
                    println!("received {:?}", cmd);
                    let symbol = match &cmd {
                        EngineCommand::CancelOrder { symbol, .. } => symbol,
                        EngineCommand::PlaceOrder { symbol, .. } => symbol,
                        EngineCommand::GetOrderBook { symbol } => symbol,
                    };

                    let sender = actors.entry(symbol.clone()).or_insert_with(|| {
                        let (tx, rx) = mpsc::channel(100);
                        tokio::spawn(actor_loop(rx));
                        tx
                    });

                    let _ = sender.send(cmd).await;
                } else {
                    //TODO: lets see what we have to do here there will be many things i guess
                }
            }
            Err(e) => {
                eprintln!("{:#?}", e);
            }
        }
    }
}

async fn actor_loop(mut rx: Receiver<EngineCommand>) {
    let mut orderbook = OrderBook::new();

    while let Some(cmd) = rx.recv().await {
        match cmd {
            EngineCommand::PlaceOrder {
                symbol,
                user_id,
                side,
                order_type,
                price,
                quantity,
            } => {
                let new_order_id = Ulid::new().to_string();
                let mut created_order = Order {
                    symbol,
                    user_id,
                    order_id: new_order_id.clone(),
                    order_type,
                    side,
                    timestamp: Utc::now().timestamp() as u64,
                    price,
                    quantity,
                    filled: Vec::new(),
                    filled_qty: 0,
                    status: OrderStatus::Open,
                };

                let fills = orderbook.match_order(&mut created_order);
                println!(
                    "Order {}: {} fills, {}/{} filled",
                    new_order_id,
                    fills.len(),
                    created_order.filled_qty,
                    created_order.quantity
                );
            }
            EngineCommand::CancelOrder { order_id, .. } => {
                orderbook.remove_order(&order_id);
                println!("Cancelled {}", order_id);
            }
            EngineCommand::GetOrderBook { symbol } => {
                let (bids, asks) = orderbook.get_depth();
                println!("{}: {} bids, {} asks", symbol, bids.len(), asks.len());
            }
        }
    }
}
