use prometheus::{
    register_histogram, register_int_counter, register_int_gauge, Histogram, IntCounter, IntGauge,
};
use std::sync::OnceLock;

pub struct IndexerMetrics {
    pub transactions_processed: IntCounter,
    pub blocks_processed: IntCounter,
    pub finalized_blocks_processed: IntCounter,
    pub purged_blocks_processed: IntCounter,
    pub traverse_errors: IntCounter,
    pub neon_transactions_saved: IntCounter,
    pub holders_saved: IntCounter,
    pub database_errors: IntCounter,
    pub parsing_errors: IntCounter,
    pub current_slot: IntGauge,
    pub holders_in_memory: IntGauge,
    pub traverse_channel_capacity: IntGauge,
    pub neon_parse_time: Histogram,
    pub holder_fetch_time: Histogram,
    pub transaction_processing_time: Histogram,
    pub block_processing_time: Histogram,
    pub finalized_block_processing_time: Histogram,
    pub purged_block_processing_time: Histogram,
}

impl IndexerMetrics {
    pub fn expose(&self, addr: std::net::SocketAddr) -> Result<(), anyhow::Error> {
        prometheus_exporter::start(addr)?;
        tracing::info!("Prometheus metrics exposed at http://{}", addr);
        Ok(())
    }
}

pub fn metrics() -> &'static IndexerMetrics {
    static METRICS: OnceLock<IndexerMetrics> = OnceLock::new();

    METRICS.get_or_init(|| IndexerMetrics {
        transactions_processed: register_int_counter!(
            "indexer_transactions_processed",
            "Number of transactions processed"
        )
        .unwrap(),
        blocks_processed: register_int_counter!(
            "indexer_blocks_processed",
            "Number of blocks processed"
        )
        .unwrap(),
        finalized_blocks_processed: register_int_counter!(
            "indexer_finalized_blocks_processed",
            "Number of finalized blocks processed"
        )
        .unwrap(),
        purged_blocks_processed: register_int_counter!(
            "indexer_purged_blocks_processed",
            "Number of purged blocks processed"
        )
        .unwrap(),
        traverse_errors: register_int_counter!(
            "indexer_traverse_errors",
            "Number of errors while traversing blocks"
        )
        .unwrap(),
        neon_transactions_saved: register_int_counter!(
            "indexer_neon_transactions_saved",
            "Number of neon transactions added/updated"
        )
        .unwrap(),
        holders_saved: register_int_counter!(
            "indexer_holders_saved",
            "Number of holders added/updated"
        )
        .unwrap(),
        database_errors: register_int_counter!(
            "indexer_database_errors",
            "Number of errors while interacting with the database"
        )
        .unwrap(),
        parsing_errors: register_int_counter!("indexer_parsing_errors", "Number of parsing errors")
            .unwrap(),
        current_slot: register_int_gauge!("indexer_current_slot", "Current slot").unwrap(),
        holders_in_memory: register_int_gauge!(
            "indexer_holders_in_memory",
            "Number of holders cached in memory"
        )
        .unwrap(),
        traverse_channel_capacity: register_int_gauge!(
            "indexer_traverse_channel_capacity",
            "Capacity of the traverse channel"
        )
        .unwrap(),
        neon_parse_time: register_histogram!(
            "indexer_neon_parse_time",
            "Time taken to parse a neon transaction"
        )
        .unwrap(),
        holder_fetch_time: register_histogram!(
            "indexer_holder_fetch_time",
            "Time taken to fetch a holder from the database"
        )
        .unwrap(),
        transaction_processing_time: register_histogram!(
            "indexer_transaction_processing_time",
            "Time taken to process a solana transaction"
        )
        .unwrap(),
        block_processing_time: register_histogram!(
            "indexer_block_processing_time",
            "Time taken to process a block"
        )
        .unwrap(),
        finalized_block_processing_time: register_histogram!(
            "indexer_finalized_block_processing_time",
            "Time taken to process a finalized block"
        )
        .unwrap(),
        purged_block_processing_time: register_histogram!(
            "indexer_purged_block_processing_time",
            "Time taken to process a purged block"
        )
        .unwrap(),
    })
}
