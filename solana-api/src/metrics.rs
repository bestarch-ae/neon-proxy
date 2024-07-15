use std::sync::OnceLock;

use prometheus::{
    linear_buckets, register_histogram, register_int_counter, register_int_gauge, Histogram,
    IntCounter, IntGauge,
};

pub struct SolanaApiMetrics {
    pub traverse: TraverseMetrics,

    pub get_signatures_for_address: IntCounter,
    pub get_transaction: IntCounter,
    pub get_block: IntCounter,
    pub get_blocks: IntCounter,
}

pub struct TraverseMetrics {
    pub buffer_len: IntGauge,
    pub uncommited_buffer_len: IntGauge,
    pub cached_block: IntGauge,
    pub earliest_slot: IntGauge,
    pub last_observed_slot: IntGauge,
    pub ignored_signatures: IntCounter,

    pub transactions_per_block: Histogram,

    pub get_current_slot_time: Histogram,
    pub get_block_time: Histogram,
    pub process_transactions_time: Histogram,
    pub decode_ui_transaction_time: Histogram,
}

pub fn metrics() -> &'static SolanaApiMetrics {
    static METRICS: OnceLock<SolanaApiMetrics> = OnceLock::new();

    METRICS.get_or_init(|| SolanaApiMetrics {
        get_signatures_for_address: register_int_counter!(
            "get_signatures_for_address",
            "get_signatures_for_address"
        )
        .unwrap(),
        get_transaction: register_int_counter!("get_transaction", "get_transaction").unwrap(),
        get_block: register_int_counter!("get_block", "get_block").unwrap(),
        get_blocks: register_int_counter!("get_blocks", "get_blocks").unwrap(),
        traverse: TraverseMetrics {
            buffer_len: register_int_gauge!("buffer_len", "Length of unprocessed signature buffer")
                .unwrap(),
            uncommited_buffer_len: register_int_gauge!(
                "uncommited_buffer_len",
                "Temporary traverse buffer length"
            )
            .unwrap(),
            cached_block: register_int_gauge!("cached_block", "Cached Block slot").unwrap(),
            earliest_slot: register_int_gauge!(
                "earliest_slot",
                "Slot of the earliest signature encountered in current traverse call"
            )
            .unwrap(),
            last_observed_slot: register_int_gauge!(
                "last_observed_slot",
                "Slot of the last observed signature"
            )
            .unwrap(),
            ignored_signatures: register_int_counter!(
                "ignored_signatures",
                "Number of signatures ignored"
            )
            .unwrap(),
            transactions_per_block: register_histogram!(
                "transactions_per_block",
                "Number of transactions per block",
                linear_buckets(1., 1., 10).unwrap()
            )
            .unwrap(),
            get_current_slot_time: register_histogram!(
                "get_current_slot_time",
                "Get current slot time"
            )
            .unwrap(),
            get_block_time: register_histogram!("get_block_time", "Get block time").unwrap(),
            process_transactions_time: register_histogram!(
                "process_transactions_time",
                "Process transactions time"
            )
            .unwrap(),
            decode_ui_transaction_time: register_histogram!(
                "decode_ui_transaction_time",
                "Decode UI transaction time"
            )
            .unwrap(),
        },
    })
}
