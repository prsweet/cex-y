use std::collections::HashMap;

use krafka::consumer::{AutoOffsetReset, Consumer};
use tokio::sync::mpsc;

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
        .expect("failed to make kafka consumer");

    consumer.subscribe(&["orders"]).await.expect("failed to subscribe kafka consumer");
    let (bal_cmd_tx, bal_cmd_rx) = mpsc::channel::<BalanceCommand>(10000);
    let (bal_event_tx, bal_event_rx) = mpsc::channel::<BalanceEvent>(10000);
    let  balance_actor = BalanceActor::new(bal_cmd_rx, bal_event_rx);
    tokio::spawn(balance_actor.run());
    
    let mut symbol_actors: HashMap<String, mpsc::Sender<ActorCommand>> = HashMap::new();

    loop {
        let received = match consumer.recv().await {
            Ok(msg) => msg,
            Err(e) => {
                eprintln!("Failed to receive message from Kafka!: {:#?}", e);
                continue;
            }
        };
        
        let Some(msg_str) = received.value_str() else {
            eprintln!("CRITICAL: Received message of invalid format from Kafka! Skipping.");
            continue;
        };

        let Ok(cmd) = serde_json::from_str::<ActorCommand>(msg_str) else {
            eprintln!("CRITICAL: Received invalid JSON! Skipping. Payload: {}", msg_str);
            continue;
        };

        println!("received {:?}", cmd);
        let symbol = match &cmd {
            ActorCommand::CancelOrder { symbol, .. } => symbol.clone(),
            ActorCommand::PlaceOrder { symbol, .. } => symbol.clone(),
            ActorCommand::GetOrderBook { symbol } => symbol.clone(),
        };

        let actor_tx = symbol_actors.entry(symbol.clone()).or_insert_with(|| {
            let (tx, rx) = mpsc::channel(10000);
            let actor = SymbolActor::new(symbol.clone(), rx, bal_event_tx.clone());
            tokio::spawn(actor.run());
            tx
        });

        let using_bal_cmd_tx = bal_cmd_tx.clone();
        let using_actor_tx = actor_tx.clone();
        
        match cmd {
            ActorCommand::PlaceOrder { symbol, user_id, side, order_type, price, quantity } => {
                let (respond_tx, respond_rx) = tokio::sync::oneshot::channel();
                tokio::spawn(async move {
                    let _ = using_bal_cmd_tx.send(BalanceCommand::CheckAndLock { 
                        user_id: user_id.clone(), 
                        symbol: symbol.clone(), 
                        side,
                        price, 
                        quantity, 
                        respond_tx
                    }).await;

                    match respond_rx.await {
                        Ok(Ok(())) => {
                            let _ = using_actor_tx.send(ActorCommand::PlaceOrder { symbol, user_id, side, order_type, price, quantity }).await;
                        },
                        Ok(Err(e)) => {
                            eprintln!("Order Rejected for {}: Insufficient Funds (Error: {})", user_id, e);
                        },
                        Err(e) => {
                            eprintln!("FATAL: BalanceActor failed to respond! Channel dropped. Error: {:#?}", e);
                        }
                    }
                });
            },
            _ => {
                let _ = using_actor_tx.send(cmd).await;
            }
        }
        
    }
    
}