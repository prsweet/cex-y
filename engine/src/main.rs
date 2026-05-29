mod types;

use chrono::Utc;
use redis::TypedCommands;
use ulid::Ulid;
use crate::types::*;

fn main()
{
    let mut db = EngineDB::new();
    let client = redis::Client::open("redis://127.0.0.1/").expect("failed to connect redis");
    let mut con = client.get_connection().expect("failed to get tht connection");
    loop {
        let received = con.brpop("order_queue", 0.0);
        if let Ok(Some([_, data])) = received {
            if let Ok(cmd) = serde_json::from_str::<EngineCommand>(&data) {
                match cmd {
                    EngineCommand::GetOrderBook { symbol } => {
                        
                    }
                    EngineCommand::CancelOrder { symbol, order_id } => {  
                        if let Some(orderbook) = db.orderbooks.get_mut(&symbol) {
                            orderbook.remove_order(&order_id);
                            if let Some(order) = db.orders.get_mut(&order_id) {
                                order.status = OrderStatus::Cancelled;
                            }
                        }
                        let event = EngineEvent::OrderCancelled { order_id };
                        let str_event = serde_json::to_string(&event).unwrap();
                        let _ = con.publish("trade_events", str_event);
                    }
                    EngineCommand::PlaceOrder { symbol, user_id, side, order_type, price, quantity } => {
                        let orderbook = db.orderbooks.entry(symbol.clone()).or_insert_with(OrderBook::new);
                        let new_order_id = Ulid::new().to_string();
                        let mut create_order = Order {
                            symbol,
                            user_id,
                            order_id: new_order_id.clone(),
                            side,
                            order_type,
                            timestamp: Utc::now().timestamp() as u64,
                            price,
                            quantity,
                            filled_qty: 0,
                            filled: Vec::new(),
                            status: types::OrderStatus::Open
                        };
                        let fills = orderbook.match_order(&mut create_order);
                        db.orders.insert(new_order_id.clone(), create_order.clone());
                        db.fill_order(fills.clone());

                        if let Some(final_order) = db.orders.get(&new_order_id) {
                            let event = EngineEvent::OrderPlaced { 
                                order: final_order.clone(), 
                                remaining: final_order.quantity - final_order.filled_qty 
                            };
                            let str_event = serde_json::to_string(&event).unwrap();
                            let _ = con.publish("trade_events", str_event);
                        }

                        for fill in fills {
                            let Some(maker_order) = db.orders.get(&fill.maker_order_id) else { continue; };
                            let maker_status = maker_order.status;
                            let maker_remaining = maker_order.quantity - maker_order.filled_qty;
                            
                            let fill_event = EngineEvent::Fill { 
                                symbol: fill.symbol, 
                                trade_id: fill.trade_id, 
                                maker_id: fill.maker_id, 
                                taker_id: fill.taker_id, 
                                price: fill.price, 
                                quantity: fill.quantity, 
                                maker_status,
                                maker_remaining
                            };

                            let str_fill = serde_json::to_string(&fill_event).unwrap();
                            let _ = con.publish("trade_events", str_fill);
                        }
                    }
                }
            }
        }
    }
}