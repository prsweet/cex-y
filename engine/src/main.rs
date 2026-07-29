use std::collections::HashMap;

use krafka::consumer::{AutoOffsetReset, Consumer};
use tokio::sync::mpsc::{Receiver, Sender, channel};

use crate::{actors::*, types::*};

mod types;
mod actors;

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
    let (bal_cmd_tx, bal_cmd_rx) = channel::<BalanceCommand>(0);
    let (bal_event_tx, bal_event_rx) = channel::<BalanceEvent>(0);
    let mut balance_actor = BalanceActor::new(bal_cmd_rx, bal_event_rx);
    tokio::spawn(balance_actor.run());
    
    let mut symbol_actors: HashMap<String, Sender<EngineCommand>> = HashMap::new();

    loop {
        match consumer.recv().await {
            Ok(received) => {
                if let Ok(cmd) = serde_json::from_str::<EngineCommand>(&received.value_str().unwrap()) {
                    println!("received {:?}", cmd);
                    let symbol = match &cmd {
                        EngineCommand::CancelOrder { symbol, .. } => symbol.clone(),
                        EngineCommand::PlaceOrder { symbol, .. } => symbol.clone(),
                        EngineCommand::GetOrderBook { symbol } => symbol.clone(),
                    };

                    let sender = symbol_actors.entry(symbol.clone()).or_insert_with(|| {
                        let (tx, rx) = channel(0);
                        let actor = SymbolActor::new(symbol.clone(), rx, bal_event_tx.clone());
                        tokio::spawn(actor.run());
                        tx
                    });

                    let sender_clone = sender.clone();
                    let bal_cmd_tx_clone = bal_cmd_tx.clone();

                    match cmd {
                        EngineCommand::PlaceOrder { symbol, user_id, side, order_type, price, quantity } => {
                            let (respond_tx, respond_rx) = tokio::sync::oneshot::channel();
                            tokio::spawn(async move {
                                let _ = bal_cmd_tx_clone.send(BalanceCommand::CheckAndLock { 
                                    user_id: user_id.clone(), 
                                    symbol: symbol.clone(), 
                                    side,
                                    price, 
                                    quantity, 
                                    respond_tx
                                }).await;

                                if let Ok(Ok(())) = respond_rx.await {
                                    let _ = sender_clone.send(EngineCommand::PlaceOrder { symbol, user_id, side, order_type, price, quantity }).await;
                                }
                            });
                        },
                        _ => {
                            let _ = sender_clone.send(cmd).await;
                        }
                    }
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