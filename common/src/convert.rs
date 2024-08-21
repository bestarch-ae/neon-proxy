#[cfg(feature = "reth")]
pub use reth::ToReth;

pub trait ToNeon {
    type NeonType;

    fn to_neon(self) -> Self::NeonType;
}

#[cfg(feature = "reth")]
mod reth {
    use reth_primitives::{Address, B256, U256};
    use rpc_api_types::AccessListItem;

    use super::*;

    impl ToNeon for U256 {
        type NeonType = ethnum::U256;

        fn to_neon(self) -> Self::NeonType {
            ethnum::U256::from_le_bytes(self.to_le_bytes())
        }
    }

    impl ToNeon for B256 {
        type NeonType = ethnum::U256;

        fn to_neon(self) -> Self::NeonType {
            ethnum::U256::from_ne_bytes(self.0)
        }
    }

    impl ToNeon for Address {
        type NeonType = evm_loader::types::Address;

        fn to_neon(self) -> Self::NeonType {
            evm_loader::types::Address(self.0.into())
        }
    }

    impl ToNeon for AccessListItem {
        type NeonType = neon_lib::types::AccessListItem;

        fn to_neon(self) -> Self::NeonType {
            neon_lib::types::AccessListItem {
                address: self.address.to_neon(),
                storage_keys: self
                    .storage_keys
                    .into_iter()
                    .map(|key| key.to_vec())
                    .map(TryFrom::try_from)
                    .map(|x| x.unwrap())
                    .collect(),
            }
        }
    }

    pub trait ToReth {
        type RethType;

        fn to_reth(self) -> Self::RethType;
    }

    impl ToReth for ethnum::U256 {
        type RethType = U256;

        fn to_reth(self) -> Self::RethType {
            U256::from_le_bytes(self.to_le_bytes())
        }
    }
}
