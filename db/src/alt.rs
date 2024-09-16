use anyhow::anyhow;
use common::solana_sdk::pubkey::Pubkey;
use futures_util::{Stream, TryStreamExt};

#[derive(Debug, Clone)]
pub struct AltRepo {
    pool: sqlx::PgPool,
}

impl AltRepo {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    pub async fn insert(&self, key: Pubkey) -> sqlx::Result<()> {
        sqlx::query!(
            r#"
            INSERT INTO solana_alts (address) VALUES ($1)
            ON CONFLICT DO NOTHING
            "#,
            key.as_ref()
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub fn fetch(&self) -> impl Stream<Item = sqlx::Result<Pubkey>> + '_ {
        sqlx::query!(
            r#"
                SELECT address
                FROM solana_alts
            "#,
        )
        .fetch(&self.pool)
        .and_then(|row| async move {
            Ok(Pubkey::new_from_array(row.address.try_into().map_err(
                |_| sqlx::Error::Decode(anyhow!("invalid db pubkey len").into()),
            )?))
        })
    }
}
