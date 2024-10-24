use neon_lib::commands::emulate::EmulateResponse;

pub trait EmulateResponseExt {
    fn has_external_solana_call(&self) -> bool;
}

impl EmulateResponseExt for EmulateResponse {
    fn has_external_solana_call(&self) -> bool {
        use evm_loader::error;

        self.external_solana_call
            || error::format_revert_error(&self.result)
                .map_or(false, |err| err.starts_with("External call fails"))
    }
}
