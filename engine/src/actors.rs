use std::collections::HashMap;

use chrono::Utc;
use tokio::sync::{mpsc::Receiver, mpsc::Sender};
use ulid::Ulid;

use crate::types::{BalanceCommand::{self, CheckAndLock}, BalanceEvent, BalancePool::{self, Available, Locked}, EngineCommand, Order, OrderBook, OrderStatus, Side::{self, Buy, Sell}, SymbolBalance};

pub struct BalanceActor {
    balances: HashMap<String, HashMap<String, SymbolBalance>>, // user_id -> asset -> avail, lock
    cmd_rx: Receiver<BalanceCommand>,
    event_rx: Receiver<BalanceEvent>,
}

impl BalanceActor {
    pub fn new(cmd_rx: Receiver<BalanceCommand>, event_rx: Receiver<BalanceEvent>) -> Self {
        Self {
            balances: HashMap::new(),
            cmd_rx,
            event_rx
        }
    }
    
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                biased;
                
                Some(event) = self.event_rx.recv() => {
                    self.handle_event(event);
                }

                Some(cmd) = self.cmd_rx.recv() => {
                    self.handle_command(cmd);
                }
            }
        }
    }

    fn handle_command(&mut self, cmd: BalanceCommand) {
        match cmd {
            CheckAndLock { user_id, symbol, side, price, quantity, respond_tx } => {
                let symbols: Vec<&str> = symbol.split("_").collect();
                if symbols.len() != 2 {
                    let _ = respond_tx.send(Err("Invalid symbol format".to_string()));
                    return;
                }
                let (locking_symbol, amount) = match side {
                    Side::Buy => (symbols[1], price * quantity),
                    Side::Sell => (symbols[0], quantity)
                };
                match self.deduct(&user_id, locking_symbol, amount, Available) {
                    Ok(()) => {
                        self.add(&user_id, &locking_symbol, amount, Locked);
                        let _ = respond_tx.send(Ok(()));
                    }
                    Err(e) => {
                        let _ = respond_tx.send(Err(e));
                    }
                }
            }
        }
    }

    pub fn get_balance(&self, user_id: &str, symbol: &str) -> Option<SymbolBalance> {
        self.balances.get(user_id)?.get(symbol).cloned()
    }

    pub fn deduct(&mut self, user_id: &str, symbol: &str, amount: u64, balance_pool: BalancePool) -> Result<(), String> {
        let user = self.balances.entry(user_id.to_string()).or_default();
        let balance = user.entry(symbol.to_string()).or_insert(SymbolBalance { available: 0, locked: 0 });
        match balance_pool {
            Available => {
                if balance.available >= amount {
                    balance.available = balance.available.saturating_sub(amount);
                    Ok(())
                } else {
                    Err("Insufficient available funds".to_string())
                }
            }
            Locked => {
                if balance.locked >= amount {
                    balance.locked = balance.locked.saturating_sub(amount);
                    Ok(())
                } else {
                    Err("Insufficient locked funds".to_string())
                }
            }
        }
    }

    pub fn add(&mut self, user_id: &str, symbol: &str, amount: u64, balance_pool: BalancePool) {
        let user = self.balances.entry(user_id.to_string()).or_default();
        let balance = user.entry(symbol.to_string()).or_insert(SymbolBalance { available: 0, locked: 0 });
        match balance_pool {
            Available => balance.available = balance.available.saturating_add(amount),
            Locked => balance.locked = balance.locked.saturating_add(amount)
        }
    }

    fn handle_event(&mut self, event: BalanceEvent) {
        match event {
            BalanceEvent::Fill { symbol, price, quantity, maker_user_id, taker_user_id, taker_side } => {
                let symbols: Vec<&str> = symbol.split('_').collect();
                if symbols.len() != 2 { return; }
                let buyer_paying = quantity * price;

                match taker_side {
                    Buy => {
                        let (get, from) = (symbols[0], symbols[1]);
                        let _ = self.deduct(&taker_user_id, &from, buyer_paying, Locked);
                        self.add(&taker_user_id, &get, quantity, Available);
                        
                        self.add(&maker_user_id, &from, buyer_paying, Available);
                        let _ = self.deduct(&maker_user_id, &get, quantity, Locked);
                    },
                    Sell => {
                        let (from, get) = (symbols[0], symbols[1]);
                        let _ = self.deduct(&taker_user_id, &from, quantity, Locked);
                        self.add(&taker_user_id, &get, buyer_paying, Available);
                        
                        self.add(&maker_user_id, &from, quantity, Available);
                        let _ = self.deduct(&maker_user_id, &get, buyer_paying, Locked);
                    }
                }
            },
            BalanceEvent::CancelOrder { user_id, symbol, amount } => {
                let user = self.balances.entry(user_id).or_default();
                if let Some(balance) = user.get_mut(&symbol) {
                    balance.locked = balance.locked.saturating_sub(amount);
                    balance.available += amount;
                }
            }
        }
    }
}

pub struct SymbolActor {
    symbol: String,
    orderbook: OrderBook,
    rx: Receiver<EngineCommand>,
    event_tx: Sender<BalanceEvent>
}

impl SymbolActor {
    pub fn new(symbol: String, rx: Receiver<EngineCommand>, event_tx: Sender<BalanceEvent>) -> Self {
        Self {
            symbol,
            orderbook: OrderBook::new(),
            rx,
            event_tx
        }
    }

    pub async fn run(mut self) {
        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                EngineCommand::PlaceOrder { symbol, user_id, side, order_type, price, quantity } => {
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
    
                    let fills = self.orderbook.match_order(&mut created_order);

                    for (val) in fills {
                        self.event_tx.send(BalanceEvent::Fill { 
                            symbol: val.symbol, 
                            price: val.price, 
                            quantity: val.quantity, 
                            buy_user_id: val.maker_user_id, 
                            sell_user_id: val.taker_user_id,
                        });
                    }
                    
                    println!(
                        "Order {}: {} fills, {}/{} filled",
                        new_order_id,
                        fills.len(),
                        created_order.filled_qty,
                        created_order.quantity
                    );
                }
                EngineCommand::CancelOrder { order_id, .. } => {
                    self.orderbook.remove_order(&order_id);
                    println!("Cancelled {}", order_id);
                }
                EngineCommand::GetOrderBook { symbol } => {
                    let (bids, asks) = self.orderbook.get_depth();
                    println!("{}: {} bids, {} asks", symbol, bids.len(), asks.len());
                }
            }
        }
    }
}