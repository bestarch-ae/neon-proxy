mod mock;

use std::fs::read_to_string;
use std::path::Path;
use std::sync::Arc;

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_network::TxSignerSync;
use alloy_signer_wallet::LocalWallet;
use anyhow::{Context, Result};
use jsonrpsee::core::async_trait;
use reth_primitives::TxKind;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program_test::{ProgramTest, ProgramTestContext};

use common::ethnum::U256 as NeonU256;
use common::evm_loader::account::{ContractAccount, MainTreasury, Treasury};
use common::neon_instruction::tag;
use common::neon_lib::commands::get_balance::GetBalanceResponse;
use common::neon_lib::commands::get_config::BuildConfigSimulator;
use common::neon_lib::commands::get_neon_elf::read_elf_parameters_from_account;
use common::neon_lib::rpc::{CloneRpcClient, Rpc};
use common::neon_lib::types::{Address, BalanceAddress};
use common::neon_lib::{commands, Config as NeonLibConfig};
use common::solana_sdk::account::Account;
use common::solana_sdk::account::AccountSharedData;
use common::solana_sdk::account_info::AccountInfo;
use common::solana_sdk::bpf_loader_upgradeable::UpgradeableLoaderState;
use common::solana_sdk::instruction::{AccountMeta, Instruction};
use common::solana_sdk::program_pack::Pack;
use common::solana_sdk::pubkey;
use common::solana_sdk::pubkey::Pubkey;
use common::solana_sdk::rent::Rent;
use common::solana_sdk::signature::Keypair;
use common::solana_sdk::signature::Signature;
use common::solana_sdk::signer::{EncodableKey, Signer};
use common::solana_sdk::transaction::Transaction;
use common::solana_sdk::{bpf_loader_upgradeable, system_instruction, system_program};
use solana_api::solana_api::SolanaApi;

use super::Executor;
use crate::convert::ToReth;
use crate::neon_api::NeonApi;

use mock::BanksRpcMock;

const NEON_KEY: Pubkey = pubkey!("53DfF883gyixYNXnM7s5xhdeyV8mVk9T4i2hGV9vG9io");
const NEON_TOKEN: Pubkey = pubkey!("HPsV9Deocecw3GeZv1FkAPNCBRfuVyfw9MMwjwRe1xaU");
const CHAIN_ID: u64 = 111;

#[derive(Debug)]
struct Wallet {
    sol: Keypair,
    eth: LocalWallet,
}

impl Wallet {
    fn new() -> Self {
        let sol = Keypair::new();
        let eth = LocalWallet::from_slice(sol.secret().as_ref()).unwrap();
        Wallet { sol, eth }
    }

    fn address(&self) -> Address {
        self.eth.address().0 .0.into()
    }

    fn pubkey(&self) -> Pubkey {
        self.sol.pubkey()
    }
}

#[async_trait]
trait ContextExt {
    async fn send_instructions(
        &mut self,
        ixs: &[Instruction],
        signers: &[&Keypair],
    ) -> anyhow::Result<()>;

    #[allow(dead_code)] // could be useful l8r
    async fn confirm_transaction(&mut self, signature: &Signature) -> Result<()>;
}

#[async_trait]
impl ContextExt for ProgramTestContext {
    async fn send_instructions(
        &mut self,
        ixs: &[Instruction],
        signers: &[&Keypair],
    ) -> anyhow::Result<()> {
        let hash = self.get_new_latest_blockhash().await?;
        let tx = Transaction::new_signed_with_payer(ixs, Some(&self.payer.pubkey()), signers, hash);

        self.banks_client.process_transaction(tx).await?;
        Ok(())
    }

    async fn confirm_transaction(&mut self, signature: &Signature) -> Result<()> {
        loop {
            if let Some(status) = self.banks_client.get_transaction_status(*signature).await? {
                return match status.err {
                    None => Ok(()),
                    Some(err) => Err(err.into()),
                };
            }
        }
    }
}

async fn init_neon(ctx: &mut ProgramTestContext) -> anyhow::Result<()> {
    let rpc = BanksRpcMock(ctx.banks_client.clone());
    let payer = ctx.payer.insecure_clone();

    let neon_lib_config = NeonLibConfig {
        evm_loader: NEON_KEY,
        key_for_config: Default::default(),
        fee_payer: None,
        commitment: Default::default(),
        solana_cli_config: Default::default(),
        db_config: None,
        json_rpc_url: Default::default(),
        keypair_path: Default::default(),
    };
    let params = read_elf_parameters_from_account(&neon_lib_config, &rpc).await?;
    eprintln!("{params:#?}");

    let neon_token_kp = Keypair::read_from_file("tests/keys/neon_token_keypair.json")
        .map_err(|err| anyhow::anyhow!("{err}"))?;
    assert_eq!(neon_token_kp.pubkey(), NEON_TOKEN);

    let rent = ctx.banks_client.get_sysvar::<Rent>().await?;

    // ===== Init NEON token =====
    let ixs = [
        system_instruction::create_account(
            &ctx.payer.pubkey(),
            &neon_token_kp.pubkey(),
            rent.minimum_balance(spl_token::state::Mint::LEN),
            spl_token::state::Mint::LEN as u64,
            &spl_token::ID,
        ),
        spl_token::instruction::initialize_mint2(
            &spl_token::ID,
            &neon_token_kp.pubkey(),
            &payer.pubkey(),
            None,
            9,
        )?,
    ];
    ctx.send_instructions(&ixs, &[&payer, &neon_token_kp])
        .await?;

    // ===== Init NEON token `Deposit` pool =====
    let (deposit_authority, _) = Pubkey::find_program_address(&[b"Deposit"], &NEON_KEY);
    let ix = spl_associated_token_account::instruction::create_associated_token_account(
        &ctx.payer.pubkey(),
        &deposit_authority,
        &neon_token_kp.pubkey(),
        &spl_token::ID,
    );
    ctx.send_instructions(&[ix], &[&payer]).await?;

    // ===== Create main treasury =====
    let main_balance_address = MainTreasury::address(&NEON_KEY).0;
    let program_data_address =
        Pubkey::find_program_address(&[NEON_KEY.as_ref()], &bpf_loader_upgradeable::id()).0;

    // HACK: program test does not use upgradeable loader.
    let state = UpgradeableLoaderState::ProgramData {
        slot: 0,
        upgrade_authority_address: Some(ctx.payer.pubkey()),
    };
    let data = bincode::serialize(&state)?;
    let lamports = rent.minimum_balance(data.len());
    let acc_data = AccountSharedData::new_data(lamports, &state, &bpf_loader_upgradeable::id())?;
    ctx.set_account(&program_data_address, &acc_data);

    let accounts = vec![
        AccountMeta::new(main_balance_address, false),
        AccountMeta::new_readonly(program_data_address, false),
        AccountMeta::new_readonly(ctx.payer.pubkey(), true),
        AccountMeta::new_readonly(spl_token::id(), false),
        AccountMeta::new_readonly(system_program::id(), false),
        AccountMeta::new_readonly(spl_token::native_mint::id(), false),
        AccountMeta::new(ctx.payer.pubkey(), true),
    ];
    let ix = Instruction::new_with_bincode(NEON_KEY, &(tag::CREATE_MAIN_TREASURY), accounts);
    ctx.send_instructions(&[ix], &[&payer]).await?;

    // ====== Create auxilary treasury balances =====
    let treasury_pool_count: u32 = params
        .get("NEON_TREASURY_POOL_COUNT")
        .context("no treasury pool count")?
        .parse()?;

    for i in 0..treasury_pool_count {
        let addr = Treasury::address(&NEON_KEY, i).0;
        let ix = system_instruction::transfer(&payer.pubkey(), &addr, rent.minimum_balance(0));
        ctx.send_instructions(&[ix], &[&payer]).await?;
    }

    Ok(())
}

async fn mint_and_deposit_to_neon(
    env: &mut ProgramTestContext,
    kp: &Wallet,
    amount: u64,
) -> anyhow::Result<()> {
    let payer = env.payer.insecure_clone();
    let addr = kp.address();

    let (deposit_authority, _) = Pubkey::find_program_address(&[b"Deposit"], &NEON_KEY);
    let neon_pool =
        spl_associated_token_account::get_associated_token_address(&deposit_authority, &NEON_TOKEN);

    // Init NEON token balance
    let wallet1 =
        spl_associated_token_account::get_associated_token_address(&kp.pubkey(), &NEON_TOKEN);
    let ixs = [
        spl_associated_token_account::instruction::create_associated_token_account(
            &env.payer.pubkey(),
            &kp.pubkey(),
            &NEON_TOKEN,
            &spl_token::id(),
        ),
        spl_token::instruction::mint_to(
            &spl_token::id(),
            &NEON_TOKEN,
            &wallet1,
            &env.payer.pubkey(),
            &[],
            amount * 10_u64.pow(9),
        )?,
        spl_token::instruction::approve(
            &spl_token::id(),
            &wallet1,
            &addr.find_balance_address(&NEON_KEY, CHAIN_ID).0,
            &kp.pubkey(),
            &[],
            amount * 10_u64.pow(9),
        )?,
    ];
    env.send_instructions(&ixs, &[&payer, &kp.sol]).await?;

    // Deposit to NEON
    let mut data = vec![0; 29];
    data[0] = tag::DEPOSIT;
    data[1..21].copy_from_slice(&addr.0);
    data[21..29].copy_from_slice(&CHAIN_ID.to_le_bytes());

    let accounts = vec![
        AccountMeta::new_readonly(NEON_TOKEN, false),
        AccountMeta::new(wallet1, false),
        AccountMeta::new(neon_pool, false),
        AccountMeta::new(addr.find_balance_address(&NEON_KEY, CHAIN_ID).0, false),
        AccountMeta::new(addr.find_solana_address(&NEON_KEY).0, false),
        AccountMeta::new_readonly(spl_token::id(), false),
        AccountMeta::new(payer.pubkey(), true),
        AccountMeta::new_readonly(system_program::id(), false),
    ];

    let ix = Instruction {
        program_id: NEON_KEY,
        accounts,
        data,
    };

    env.send_instructions(&[ix], &[&payer]).await?;

    Ok(())
}

#[tokio::test]
async fn basic() -> anyhow::Result<()> {
    let mut env = ProgramTest::default();
    env.prefer_bpf(true);
    env.add_program("evm_loader", NEON_KEY, None);

    let mut env = env.start_with_context().await;
    init_neon(&mut env).await?;
    let payer = env.payer.insecure_clone();

    let banks_client = env.banks_client.clone();
    let neon_api = NeonApi::new_with_custom_rpc_clients(
        move |_| {
            let rpc = BanksRpcMock(banks_client.clone());
            RpcClient::new_sender(rpc, Default::default())
        },
        NEON_KEY,
        payer.pubkey(),
        Default::default(),
        64,
    );

    let rpc = BanksRpcMock(env.banks_client.clone());
    let solana_api = SolanaApi::with_sender(rpc);

    let rpc = BanksRpcMock(env.banks_client.clone());
    let rpc = RpcClient::new_sender(rpc, Default::default());
    let rpc = CloneRpcClient {
        rpc: Arc::new(rpc),
        key_for_config: payer.pubkey(),
        max_retries: 5,
    };

    let operator = Keypair::new();
    let operator_signer = LocalWallet::from_slice(operator.secret().as_ref())?;
    let address = operator_signer.address();
    let ix = system_instruction::transfer(&payer.pubkey(), &operator.pubkey(), 100 * 10u64.pow(9));
    env.send_instructions(&[ix], &[&payer]).await?;
    let (executor, task) = Executor::initialize_and_start(
        neon_api.clone(),
        solana_api,
        NEON_KEY,
        operator,
        address.0 .0.into(),
    )
    .await
    .context("failed initializing executor")?;
    tokio::spawn(task);
    executor.init_operator_balance(CHAIN_ID).await?.unwrap();
    executor.join_current_transactions().await;

    let kp1 = Wallet::new();
    let address1 = kp1.address();
    let kp2 = Wallet::new();
    let address2 = kp2.address();

    mint_and_deposit_to_neon(&mut env, &kp1, 1_000).await?;

    // Transfer
    let balance = get_balances(&rpc, &[address1, address2]).await?;
    assert_eq!(balance[0].balance, eth_to_wei(1_000));
    assert_eq!(balance[1].balance, eth_to_wei(0));

    let mut tx = TxLegacy {
        nonce: 0,
        gas_price: 1_000_000,
        gas_limit: u128::MAX,
        to: TxKind::Call(address2.0.into()),
        value: eth_to_wei(900).to_reth(),
        input: Default::default(),
        chain_id: Some(CHAIN_ID),
    };
    let signature = kp1.eth.sign_transaction_sync(&mut tx)?;
    let tx = tx.into_signed(signature);
    executor.handle_transaction(tx.into()).await?;

    let txs = executor.join_current_transactions().await;
    assert_eq!(txs.len(), 1);

    let balance = get_balances(&rpc, &[address1, address2]).await?;
    assert!(balance[0].balance < eth_to_wei(100));
    assert_eq!(balance[1].balance, eth_to_wei(900));

    Ok(())
}

#[tokio::test]
async fn deploy_contract() -> anyhow::Result<()> {
    let mut env = ProgramTest::default();
    env.prefer_bpf(true);
    env.add_program("evm_loader", NEON_KEY, None);

    let mut env = env.start_with_context().await;
    init_neon(&mut env).await?;
    let payer = env.payer.insecure_clone();

    let banks_client = env.banks_client.clone();
    let neon_api = NeonApi::new_with_custom_rpc_clients(
        move |_| {
            let rpc = BanksRpcMock(banks_client.clone());
            RpcClient::new_sender(rpc, Default::default())
        },
        NEON_KEY,
        payer.pubkey(),
        Default::default(),
        64,
    );

    let rpc = BanksRpcMock(env.banks_client.clone());
    let solana_api = SolanaApi::with_sender(rpc);

    let operator = Keypair::new();
    let operator_signer = LocalWallet::from_slice(operator.secret().as_ref())?;
    let address = operator_signer.address();
    let ix = system_instruction::transfer(&payer.pubkey(), &operator.pubkey(), 100 * 10u64.pow(9));
    env.send_instructions(&[ix], &[&payer]).await?;
    let (executor, task) = Executor::initialize_and_start(
        neon_api.clone(),
        solana_api,
        NEON_KEY,
        operator,
        address.0 .0.into(),
    )
    .await
    .context("failed initializing executor")?;
    tokio::spawn(task);
    executor.init_operator_balance(CHAIN_ID).await?.unwrap();
    executor.join_current_transactions().await;

    let kp = Wallet::new();
    mint_and_deposit_to_neon(&mut env, &kp, 1_000).await?;

    let code = Contract::read("tests/fixtures/hello_world")?;
    let mut tx = code.deploy_tx();
    let signature = kp.eth.sign_transaction_sync(&mut tx)?;
    let tx = tx.into_signed(signature);
    executor.handle_transaction(tx.into()).await?;

    let txs = executor.join_current_transactions().await;
    assert!(txs.len() > 1);

    let contract_address = Address::from_create(&kp.address().0.into(), 0);
    let (contract_pubkey, _) = contract_address.find_solana_address(&NEON_KEY);
    let account = env
        .banks_client
        .get_account(contract_pubkey)
        .await?
        .context("missing contract account")?;
    code.verify(contract_pubkey, account);

    Ok(())
}

async fn get_balances(
    rpc: &(impl Rpc + BuildConfigSimulator),
    addr: &[Address],
) -> Result<Vec<GetBalanceResponse>> {
    let addr: Vec<_> = addr
        .iter()
        .map(|&address| BalanceAddress {
            address,
            chain_id: CHAIN_ID,
        })
        .collect();
    let res = commands::get_balance::execute(rpc, &NEON_KEY, &addr).await?;
    Ok(res)
}

fn eth_to_wei<E>(eth: impl TryInto<NeonU256, Error = E>) -> NeonU256
where
    E: std::fmt::Debug,
{
    eth.try_into().unwrap() * NeonU256::from(10u64).pow(18)
}

struct Contract {
    init: Vec<u8>,
    runtime: Vec<u8>,
}

impl Contract {
    fn read(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let init = read_to_string(path.as_ref().with_extension("bin"))?;
        let init = hex::decode(init)?;
        let runtime = read_to_string(path.as_ref().with_extension("bin-runtime"))?;
        let runtime = hex::decode(runtime)?;
        Ok(Self { init, runtime })
    }

    fn deploy_tx(&self) -> TxLegacy {
        TxLegacy {
            nonce: 0,
            gas_price: 2,
            gas_limit: u128::MAX,
            to: TxKind::Create,
            value: eth_to_wei(0).to_reth(),
            input: self.init.clone().into(),
            chain_id: Some(CHAIN_ID),
        }
    }

    fn verify(&self, key: Pubkey, mut account: Account) {
        let account_info: AccountInfo<'_> = (&key, &mut account).into();
        let contract_account = ContractAccount::from_account(&NEON_KEY, account_info).unwrap();
        assert_eq!(&*contract_account.code(), &self.runtime);
    }
}
