#[derive(Debug)]
pub enum EngineError {
    InsufficientAvailableBalanced { required: u64, available: u64 },
    InsufficientLockedBalanced { required: u64, locked: u64 },
    OrderNotFound { order_id: String },
    InvalidSymbol { symbol: String },
    UnexpectedError { message: String },
    UserNotFound { user_id: String }
}