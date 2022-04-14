use lazy_static::lazy_static;
use prometheus::{register_int_counter, Encoder, IntCounter};

lazy_static! {
    static ref TOTAL_MESSAGES_RECEIVED: IntCounter =
        register_int_counter!("Total_messages_received", "Total messages received").unwrap();
    static ref TOTAL_MESSAGES_PROCESSED: IntCounter =
        register_int_counter!("Total_messages_processed", "Total messages processed").unwrap();
    static ref TOTAL_MESSAGES_SENT: IntCounter =
        register_int_counter!("Total_messages_sent", "Total messages sent").unwrap();
    static ref TOTAL_BAN_REAS0N: IntCounter =
        register_int_counter!("Total_ban_reason", "Total ban reason").unwrap();
}

pub enum Counter {
    MessagesReceived,
    MessagesProcessed,
    MessagesSent,
    BanReason,
}

impl Counter {
    fn inc(&self) {
        match self {
            Self::MessagesReceived => TOTAL_MESSAGES_RECEIVED.inc(),
            Self::MessagesProcessed => TOTAL_MESSAGES_PROCESSED.inc(),
            Self::MessagesSent => TOTAL_MESSAGES_SENT.inc(),
            _ => unreachable!(),
        }
    }
}

pub struct Measure {}

impl Measure {
    pub fn inc(counter: Counter) {
        counter.inc();
    }

    /// Gather metrics
    pub fn gather() -> Vec<u8> {
        let encoder = prometheus::TextEncoder::new();
        let metric_families = prometheus::gather();
        let mut buffer = vec![];
        encoder.encode(&metric_families, &mut buffer).unwrap();

        buffer
    }

    /// Push result data
    pub fn push() {
        todo!("Should be specific requirements")
    }
}
