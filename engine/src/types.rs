use std::{collections::{BTreeMap, HashMap}};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot::Sender;
use ulid::Ulid;

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum Side { Buy, Sell }

#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub enum OrderType { Limit, Market }

#[derive(Debug, Serialize, Clone, Copy)]
pub enum OrderStatus { PartiallyFilled, Open, Cancelled, Filled }

#[derive(Debug)]
pub struct RestingNode {
    order: RestingOrder,
    prev_node: Option<String>,
    next_node: Option<String>
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolBalance {
    pub available: u64,
    pub locked: u64
}

#[derive(Debug, Clone, Copy)]
pub enum BalancePool {
    Available,
    Locked
}

#[derive(Debug)]
pub struct PriceLevel {
    head: Option<String>,
    tail: Option<String>,
    total_qty: u64
}

#[derive(Debug, Serialize, Clone)]
pub struct Fill {
    pub symbol: String,
    pub trade_id: String,
    pub buy_user_id: String,
    pub sell_user_id: String,
    pub maker_order_id: String,
    pub taker_order_id: String,
    pub price: u64,
    pub quantity: u64
}

#[derive(Debug, Serialize, Clone)]
pub struct Order {
    pub symbol: String,
    pub user_id: String,
    pub order_id: String,
    pub side: Side,
    pub order_type: OrderType,
    pub price: u64,
    pub quantity: u64,
    pub filled_qty: u64,
    pub filled: Vec<Fill>,
    pub timestamp: u64,
    pub status: OrderStatus
}

#[derive(Debug)]
pub struct RestingOrder {
    pub symbol: String,
    pub order_id: String,
    pub user_id: String,
    pub side: Side,
    pub price: u64,
    pub qty: u64,
    pub timestamp: u64,
}

#[derive(Debug)]
pub struct OrderBook {
    pub bids: BTreeMap<u64, PriceLevel>,
    pub asks: BTreeMap<u64, PriceLevel>,
    order_nodes: HashMap<String, RestingNode>
}

impl OrderBook {
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            order_nodes: HashMap::new()
            
        }
    }

    pub fn add_resting_order(&mut self, order: RestingOrder) {
        let order_id = order.order_id.clone();
        let price = order.price;
        let side = &order.side;

        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks
        };

        let level = levels.entry(price).or_insert(PriceLevel { 
            head: None, 
            tail: None,
            total_qty: 0,
        });

        level.total_qty += order.qty;

        match &level.tail {
            None => {
                level.head = Some(order_id.clone());
                level.tail = Some(order_id.clone());

                self.order_nodes.insert(order_id.clone(), RestingNode {
                    order,
                    prev_node: None,
                    next_node: None,
                });
            }
            Some(prev_tail_id) => {
                if let Some(prev_tail) = self.order_nodes.get_mut(prev_tail_id) {
                    prev_tail.next_node = Some(order_id.clone());
                }

                self.order_nodes.insert(order_id.clone(), RestingNode {
                    order,
                    prev_node: Some(prev_tail_id.clone()),
                    next_node: None,
                });

                level.tail = Some(order_id.clone());
            }
        }
    }

    pub fn match_order(&mut self, created_order: &mut Order) -> Vec<Fill> {
        let mut fills: Vec<Fill> = Vec::new();
        while created_order.quantity > created_order.filled_qty {
            let book_side = match created_order.side {
                Side::Buy => &self.asks,
                Side::Sell => &self.bids
            };
            
            let Some(best_level) = match created_order.side {
                Side::Buy => book_side.first_key_value(),
                Side::Sell => book_side.last_key_value()
            }.map(|(_, level)| level) else { break };

            let Some(order_id) = best_level.head.clone() else { break; };

            let (fully_filled, resting_order_id) = {
                let Some(resting_node) = self.order_nodes.get_mut(&order_id) else { break; };
                let resting_order = &mut resting_node.order;
                
                let is_match = match created_order.order_type {
                    OrderType::Market => true,
                    OrderType::Limit => {
                        match created_order.side {
                            Side::Buy => created_order.price >= resting_order.price,
                            Side::Sell => created_order.price <= resting_order.price
                        }
                    }
                };
                
                if !is_match { break };
                
                let remain = created_order.quantity - created_order.filled_qty;
                let filled_qty = remain.min(resting_order.qty);
                let matched_price = resting_order.price;
    
                fills.push(Fill {
                    symbol: created_order.symbol.clone(),
                    trade_id: Ulid::new().to_string(),
                    maker_user_id: resting_order.user_id.clone(),
                    taker_user_id: created_order.user_id.clone(),
                    maker_order_id: resting_order.order_id.clone(),
                    taker_order_id: created_order.order_id.clone(),
                    price: matched_price,
                    quantity: filled_qty
                });

                if let Some(last_fill) = fills.last() {
                    created_order.filled.push(last_fill.clone());
                    created_order.status = OrderStatus::PartiallyFilled;
                }
                created_order.filled_qty += filled_qty;
                resting_order.qty -= filled_qty;

                let book_side = match created_order.side {
                    Side::Buy => &mut self.asks,
                    Side::Sell => &mut self.bids
                };

                if let Some(mut level) = match created_order.side {
                    Side::Buy => book_side.first_entry(),
                    Side::Sell => book_side.last_entry()
                } {
                    level.get_mut().total_qty -= filled_qty;
                }

                (resting_order.qty == 0, resting_order.order_id.clone())
            };            
            
            if fully_filled { 
                self.remove_order(&resting_order_id);
            }
        }

        if created_order.filled_qty < created_order.quantity {
            match created_order.order_type {
                OrderType::Limit => {
                    let new_resting_order = RestingOrder {
                        symbol: created_order.symbol.clone(),
                        order_id: created_order.order_id.clone(),
                        user_id: created_order.user_id.clone(),
                        side: created_order.side.clone(),
                        price: created_order.price,
                        qty: created_order.quantity - created_order.filled_qty,
                        timestamp: created_order.timestamp
                    };
        
                    self.add_resting_order(new_resting_order);
                }
                OrderType::Market => {
                    created_order.status = OrderStatus::Cancelled;
                }
            }
        } else {
            created_order.status = OrderStatus::Filled;
        }
        
        fills
    }

    pub fn remove_order(&mut self, order_id: &str) -> Option<RestingNode> {
        let resting_node = self.order_nodes.remove(order_id)?;
        
        let book_side = match resting_node.order.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks
        };
        
        let level = book_side.get_mut(&resting_node.order.price)
            .expect("OrderBook corrupted: Price level not found for the removing order");

        level.total_qty -= resting_node.order.qty;

        match resting_node.next_node.clone() {
            Some(next_id) => {
                if let Some(next_node) = self.order_nodes.get_mut(&next_id) {
                    next_node.prev_node = resting_node.prev_node.clone();
                }
            }
            None => {
                level.tail = resting_node.prev_node.clone();
            }
        };

        match resting_node.prev_node.clone() {
            Some(prev_id) => {
                if let Some(prev_node) = self.order_nodes.get_mut(&prev_id) {
                    prev_node.next_node = resting_node.next_node.clone();
                }
            }
            None => {
                level.head = resting_node.prev_node.clone();
            }
        };

        if level.head.is_none() && level.tail.is_none() {
            book_side.remove_entry(&resting_node.order.price);
        }

        Some(resting_node)
    }

    pub fn get_depth(&self) -> (Vec<(u64, u64)>, Vec<(u64, u64)>) {
        let bids: Vec<(u64, u64)> = self.bids.iter()
            .map(|(&price, level)| { (price, level.total_qty) })
            .rev()
            .take(50)
            .collect();

        let asks: Vec<(u64, u64)> = self.asks.iter()
            .map(|(&price, level)| { (price, level.total_qty) })
            .take(50)
            .collect();

        (bids, asks)
    }
}

#[derive(Debug)]
pub struct EngineDB {
    pub orderbooks: HashMap<String, OrderBook>,
    pub orders: HashMap<String, Order>
}

impl EngineDB {
    pub fn new() -> Self {
        Self { 
            orderbooks: HashMap::new(),
            orders: HashMap::new() 
        }
    }

    pub fn fill_order(&mut self, fills: Vec<Fill>) {
        for val in fills {
            let maker = self.orders.get_mut(&val.maker_order_id);

            if let Some(maker) = maker {
                maker.filled.push(val.clone());
                maker.filled_qty += val.quantity;
                if maker.filled_qty < maker.quantity {
                    maker.status = OrderStatus::PartiallyFilled
                } else {
                    maker.status = OrderStatus::Filled
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum EngineCommand {
    PlaceOrder {
        symbol: String,
        user_id: String,
        side: Side,
        order_type: OrderType,
        price: u64,
        quantity: u64,
    },
    CancelOrder {
        symbol: String,
        order_id: String
    },
    GetOrderBook {
        symbol: String
    }
}

pub enum BalanceCommand {
    CheckAndLock {
        user_id: String,
        symbol: String,
        side: Side,
        price: u64,
        quantity: u64,
        respond_tx: Sender<Result<(), String>>
    }
}

pub enum BalanceEvent {
    Fill {
        symbol: String,
        price: u64,
        quantity: u64,
        buy_user_id: String,
        sell_user_id: String,
    },
    CancelOrder {
        user_id: String,
        symbol: String,
        amount: u64
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub enum EngineEvent {
    OrderPlaced {
        order: Order,
        remaining: u64
    },
    OrderBookDepth {
      symbol: String,
      bids: Vec<(u64, u64)>,
      asks: Vec<(u64, u64)>,
    },
    OrderCancelled {
        order_id: String
    },
    Error {
        message: String
    }
}