use std::collections::HashMap;

use chrono::Utc;
use tokio::sync::{mpsc::Receiver, mpsc::Sender};
use ulid::Ulid;

use crate::{errors::EngineError, types::{ActorCommand, BalanceCommand, BalanceEvent, BalancePool, Order, OrderBook, OrderStatus, Side, SymbolBalance}};

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
            BalanceCommand::CheckAndLock { user_id, symbol, side, price, quantity, respond_tx } => {
                let symbols: Vec<&str> = symbol.split("_").collect();
                if symbols.len() != 2 {
                    let _ = respond_tx.send(Err(EngineError::InvalidSymbol { symbol }));
                    return;
                }
                let (locking_symbol, amount) = match side {
                    Side::Buy => (symbols[1], price * quantity),
                    Side::Sell => (symbols[0], quantity)
                };
                match self.deduct(&user_id, locking_symbol, amount, BalancePool::Available) {
                    Ok(()) => {
                        self.add(&user_id, &locking_symbol, amount, BalancePool::Locked);
                        let _ = respond_tx.send(Ok(()));
                    },
                    Err(EngineError::InsufficientAvailableBalanced { required, available }) => {
                        let _ = respond_tx.send(Err(EngineError::InsufficientAvailableBalanced { required, available }));
                    }
                    Err(e) => {
                        let _ = respond_tx.send(Err(EngineError::UnexpectedError { message: format!("{:#?}", e) }));
                    }
                }
            },
            BalanceCommand::GetBalance { user_id, symbol, respond_tx } => {
                let _ = respond_tx.send(self.get_balance(&user_id, &symbol));
            }
        }
    }

    pub fn get_balance(&self, user_id: &str, symbol: &str) -> Result<SymbolBalance, EngineError> {
        let Some(user) = self.balances.get(user_id) else {
            return Err(EngineError::UserNotFound { user_id: user_id.to_string() });
        };

        let Some(bal) = user.get(symbol) else {
            return Err(EngineError::InvalidSymbol { symbol: symbol.to_string() });
        };

        Ok(bal.clone())
    }

    pub fn deduct(&mut self, user_id: &str, symbol: &str, amount: u64, balance_pool: BalancePool) -> Result<(), EngineError> {
        let user = self.balances.entry(user_id.to_string()).or_default();
        let balance = user.entry(symbol.to_string()).or_insert(SymbolBalance { available: 0, locked: 0 });
        match balance_pool {
            BalancePool::Available => {
                if balance.available >= amount {
                    balance.available = balance.available.saturating_sub(amount);
                    Ok(())
                } else {
                    Err(EngineError::InsufficientAvailableBalanced { required: amount, available: balance.available })
                }
            }
            BalancePool::Locked => {
                if balance.locked >= amount {
                    balance.locked = balance.locked.saturating_sub(amount);
                    Ok(())
                } else {
                    Err(EngineError::InsufficientLockedBalanced { required: amount, locked: balance.locked })
                }
            }
        }
    }

    pub fn add(&mut self, user_id: &str, symbol: &str, amount: u64, balance_pool: BalancePool) {
        let user = self.balances.entry(user_id.to_string()).or_default();
        let balance = user.entry(symbol.to_string()).or_insert(SymbolBalance { available: 0, locked: 0 });
        match balance_pool {
            BalancePool::Available => balance.available = balance.available.saturating_add(amount),
            BalancePool::Locked => balance.locked = balance.locked.saturating_add(amount)
        }
    }

    fn handle_event(&mut self, event: BalanceEvent) {
        match event {
            BalanceEvent::Fill { symbol, price, quantity, buy_user_id, sell_user_id} => {
                let symbols: Vec<&str> = symbol.split('_').collect();
                if symbols.len() != 2 { return; }
                let buyer_paying = quantity * price;

                let (get, from) = (symbols[0], symbols[1]);
                let _ = self.deduct(&buy_user_id, &from, buyer_paying, BalancePool::Locked);
                self.add(&buy_user_id, &get, quantity, BalancePool::Available);
                
                self.add(&sell_user_id, &from, buyer_paying, BalancePool::Available);
                let _ = self.deduct(&sell_user_id, &get, quantity, BalancePool::Locked);
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
    rx: Receiver<ActorCommand>,
    event_tx: Sender<BalanceEvent>
}

impl SymbolActor {
    pub fn new(symbol: String, rx: Receiver<ActorCommand>, event_tx: Sender<BalanceEvent>) -> Self {
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
                ActorCommand::PlaceOrder { symbol, user_id, side, order_type, price, quantity } => {
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

                    for val in &fills {
                        let _ = self.event_tx.send(BalanceEvent::Fill { 
                            symbol: val.symbol.clone(), 
                            price: val.price, 
                            quantity: val.quantity, 
                            buy_user_id: val.buy_user_id.clone(), 
                            sell_user_id: val.sell_user_id.clone(),
                        }).await;
                    }
                    
                    println!(
                        "Order {}: {} fills, {}/{} filled",
                        new_order_id,
                        fills.len(),
                        created_order.filled_qty,
                        created_order.quantity
                    );
                }
                ActorCommand::CancelOrder { symbol, order_id }=> {
                    let Some(node) = self.orderbook.remove_order(&order_id) else { return };
                    let order = node.order;
                    let amount = match order.side {
                        Side::Buy => order.qty * order.price,
                        Side::Sell => order.qty
                    };
                    let _ = self.event_tx.send(BalanceEvent::CancelOrder { user_id: order.user_id, symbol, amount }).await;
                    println!("Cancelled {}", order_id);
                }
                ActorCommand::GetOrderBook { symbol } => {
                    let (bids, asks) = self.orderbook.get_depth();
                    println!("{}: {} bids, {} asks", symbol, bids.len(), asks.len());
                }
            }
        }
    }
}