mod mock;

use std::fs::read_to_string;
use std::path::Path;
use std::sync::Arc;

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_network::TxSignerSync;
use alloy_signer::Signature as EthSignature;
use alloy_signer_wallet::LocalWallet;
use alloy_sol_types::SolConstructor;
use alloy_sol_types::{sol, SolCall};
use anyhow::{Context, Result};
use async_trait::async_trait;
use ethnum::U256 as NeonU256;
use evm_loader::account::{
    ContractAccount, MainTreasury, Treasury, TAG_HOLDER, TAG_STATE, TAG_STATE_FINALIZED,
};
use neon_lib::commands::get_balance::GetBalanceResponse;
use neon_lib::commands::get_config::BuildConfigSimulator;
use neon_lib::commands::get_neon_elf::read_elf_parameters_from_account;
use neon_lib::rpc::{CloneRpcClient, Rpc};
use neon_lib::types::{Address, BalanceAddress};
use neon_lib::{commands, Config as NeonLibConfig};
use reth_primitives::{TxKind, U256};
use serial_test::serial;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program_test::{find_file, read_file, ProgramTest, ProgramTestContext};
use solana_sdk::account::Account;
use solana_sdk::account_info::AccountInfo;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::program_pack::Pack;
use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::rent::Rent;
use solana_sdk::signature::Keypair;
use solana_sdk::signature::Signature;
use solana_sdk::signer::{EncodableKey, Signer};
use solana_sdk::transaction::Transaction;
use solana_sdk::{bpf_loader_upgradeable, system_instruction, system_program};

use common::convert::ToReth;
use common::neon_instruction::tag;
use neon_api::NeonApi;
use operator::Operator;
use solana_api::solana_api::SolanaApi;

use self::mock::BanksRpcMock;
use crate::{Execute, ExecuteRequest, Executor, HOLDER_SIZE};

const NEON_KEY: Pubkey = pubkey!("53DfF883gyixYNXnM7s5xhdeyV8mVk9T4i2hGV9vG9io");
const NEON_TOKEN: Pubkey = pubkey!("HPsV9Deocecw3GeZv1FkAPNCBRfuVyfw9MMwjwRe1xaU");
const FST_HOLDER_KEY: Pubkey = pubkey!("9X4CgVP88B3LeoX7oTmhj7vdkayBG9k73drCh2e4A61G");
const CHAIN_ID: u64 = 111;
const MAX_HOLDERS: u8 = 10;

#[derive(Debug)]
struct Wallet {
    sol: Keypair,
    eth: LocalWallet,
}

impl Wallet {
    fn new() -> Self {
        Self::from_keypair(Keypair::new())
    }

    fn from_keypair(sol: Keypair) -> Self {
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

    async fn add_program(&mut self, name: &str, pubkey: Pubkey) -> Result<()>;

    async fn sub_payer_balance(&mut self, amount: u64) -> Result<()>;
}

#[async_trait]
impl ContextExt for ProgramTestContext {
    async fn send_instructions(
        &mut self,
        ixs: &[Instruction],
        signers: &[&Keypair],
    ) -> anyhow::Result<()> {
        let hash = self.banks_client.get_latest_blockhash().await?;
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

    async fn add_program(&mut self, name: &str, pubkey: Pubkey) -> Result<()> {
        use bpf_loader_upgradeable::UpgradeableLoaderState;
        let code_file = find_file(name).context("cannot find old evm_loader")?;
        let mut data = read_file(code_file);

        let rent = self.banks_client.get_rent().await?;
        let (data_key, _) =
            Pubkey::find_program_address(&[pubkey.as_ref()], &bpf_loader_upgradeable::id());
        let acc_data = UpgradeableLoaderState::Program {
            programdata_address: data_key,
        };
        let balance = rent.minimum_balance(UpgradeableLoaderState::size_of_program());
        self.sub_payer_balance(balance).await?;

        let mut account = Account::new_data(balance, &acc_data, &bpf_loader_upgradeable::id())?;
        account.executable = true;
        self.set_account(&pubkey, &account.into());
        let acc_data = UpgradeableLoaderState::ProgramData {
            slot: 0,
            upgrade_authority_address: Some(self.payer.pubkey()),
        };
        if data.len() < 5 * 1024 * 1024 {
            data.resize(5 * 1024 * 1024, 0);
        }
        let balance = rent.minimum_balance(UpgradeableLoaderState::size_of_programdata(data.len()));
        self.sub_payer_balance(balance).await?;
        let mut account = Account::new_data(balance, &acc_data, &bpf_loader_upgradeable::id())?;
        account.data.extend(data);
        self.set_account(&data_key, &account.into());

        Ok(())
    }

    async fn sub_payer_balance(&mut self, amount: u64) -> Result<()> {
        let mut acc = self
            .banks_client
            .get_account(self.payer.pubkey())
            .await?
            .context("no payer account")?;
        acc.lamports -= amount;
        self.set_account(&self.payer.pubkey(), &acc.into());
        Ok(())
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

struct ExecutorTestEnvironment {
    test_ctx: ProgramTestContext,
    rpc: CloneRpcClient,
    test_kp: Wallet,
    executor: Arc<Executor>,
    neon_api: NeonApi,
    solana_api: SolanaApi,
}

impl ExecutorTestEnvironment {
    async fn start() -> Result<Self> {
        Self::start_with_program("evm_loader.so").await
    }

    async fn start_with_program(path: &str) -> Result<Self> {
        let mut ctx = ProgramTest::default();

        ctx.prefer_bpf(true);

        let mut ctx = ctx.start_with_context().await;
        ctx.add_program(path, NEON_KEY).await?;
        init_neon(&mut ctx).await?;

        let payer = ctx.payer.insecure_clone();

        let banks_client = ctx.banks_client.clone();
        let mock = BanksRpcMock(banks_client.clone());
        let rpc = mock.clone();
        let neon_api = NeonApi::new_with_custom_rpc_clients(
            move |_| RpcClient::new_sender(rpc.clone(), Default::default()),
            NEON_KEY,
            payer.pubkey(),
            Default::default(),
            64,
            None,
        );

        let rpc = mock.clone();
        let solana_api = SolanaApi::with_sender(rpc);

        let rpc = mock.clone();
        let rpc = RpcClient::new_sender(rpc, Default::default());
        let rpc = CloneRpcClient {
            rpc: Arc::new(rpc),
            key_for_config: payer.pubkey(),
            max_retries: 5,
        };

        let operator = Operator::read_from_file("tests/keys/operator.json").map(Arc::new)?;
        let ix =
            system_instruction::transfer(&payer.pubkey(), &operator.pubkey(), 100 * 10u64.pow(9));
        ctx.send_instructions(&[ix], &[&payer]).await?;
        let (executor, _task) = Executor::builder()
            .neon_pubkey(NEON_KEY)
            .operator(operator.clone())
            .neon_api(neon_api.clone())
            .solana_api(solana_api.clone())
            .init_operator_balance(false)
            .max_holders(MAX_HOLDERS)
            .holder_size(HOLDER_SIZE)
            .prepare()
            .start()
            .await?;
        executor.init_operator_balance(CHAIN_ID).await?.unwrap();
        executor.join_current_transactions().await;

        let kp = Wallet::new();
        mint_and_deposit_to_neon(&mut ctx, &kp, 1_000).await?;

        let env = Self {
            test_ctx: ctx,
            rpc,
            test_kp: kp,
            executor,
            neon_api,
            solana_api,
        };
        Ok(env)
    }
}

#[tokio::test]
#[serial]
async fn transfer() -> Result<()> {
    println!("started transfer");
    let ExecutorTestEnvironment {
        rpc,
        test_kp: kp1,
        executor,
        ..
    } = ExecutorTestEnvironment::start().await?;

    let kp2 = Wallet::new();
    let address2 = kp2.address();

    // Transfer
    do_transfer(&executor, &rpc, &kp1, address2, 900, 2).await
}

#[tokio::test]
#[serial]
async fn transfer_deprecated() -> Result<()> {
    println!("started transfer_deprecated");
    let ExecutorTestEnvironment {
        rpc,
        test_kp: kp1,
        executor,
        ..
    } = ExecutorTestEnvironment::start_with_program("evm_loader-1.14.5.so").await?;

    let kp2 = Wallet::new();
    let address2 = kp2.address();

    // Transfer
    do_transfer(&executor, &rpc, &kp1, address2, 900, 1).await
}

async fn do_transfer(
    executor: &Executor,
    rpc: &CloneRpcClient,
    from: &Wallet,
    to: Address,
    amount: u64,
    expected_txs: usize,
) -> Result<()> {
    let address1 = from.address();
    let address2 = to;

    // Transfer
    let balance = get_balances(rpc, &[address1, address2]).await?;
    let init_balance = balance[0].balance;
    assert_eq!(balance[1].balance, eth_to_wei(0));

    let mut tx = TxLegacy {
        nonce: 0,
        gas_price: 2,
        gas_limit: 2_000_000,
        to: TxKind::Call(address2.0.into()),
        value: eth_to_wei(amount).to_reth(),
        input: Default::default(),
        chain_id: Some(CHAIN_ID),
    };
    let signature = from.eth.sign_transaction_sync(&mut tx)?;
    let tx = tx.into_signed(signature);
    executor.handle_transaction(req(tx), None).await?;

    let txs = executor.join_current_transactions().await;
    assert_eq!(txs.len(), expected_txs);

    let balance = get_balances(rpc, &[address1, address2]).await?;
    assert!(balance[0].balance < init_balance - eth_to_wei(amount));
    assert_eq!(balance[1].balance, eth_to_wei(amount));

    Ok(())
}

#[tokio::test]
#[serial]
async fn transfer_no_chain_id() -> Result<()> {
    println!("started transfer_no_chain_id");
    let ExecutorTestEnvironment {
        rpc,
        test_kp: kp1,
        executor,
        mut test_ctx,
        ..
    } = ExecutorTestEnvironment::start().await?;

    let address1 = kp1.address();
    let kp2 = Wallet::new();
    let address2 = kp2.address();

    // Transfer
    let balance = get_balances(&rpc, &[address1, address2]).await?;
    assert_eq!(balance[0].balance, eth_to_wei(1_000));
    assert_eq!(balance[1].balance, eth_to_wei(0));

    executor
        .handle_transaction(build_transfer_no_chain_id(&kp1, address2)?, None)
        .await?;

    // HACK: Fixes random AccountInUse error
    let _ = test_ctx.banks_client.get_account(FST_HOLDER_KEY).await?;
    let txs = executor.join_current_transactions().await;
    assert!(txs.len() > 1);

    let balance = get_balances(&rpc, &[address1, address2]).await?;
    assert!(balance[0].balance < eth_to_wei(100));
    assert_eq!(balance[1].balance, eth_to_wei(900));

    Ok(())
}

fn build_transfer_no_chain_id(kp1: &Wallet, addr2: Address) -> anyhow::Result<ExecuteRequest> {
    let mut tx = TxLegacy {
        nonce: 0,
        gas_price: 2,
        gas_limit: 2_000,
        to: TxKind::Call(addr2.0.into()),
        value: eth_to_wei(900).to_reth(),
        input: Default::default(),
        chain_id: None,
    };
    let signature = kp1.eth.sign_transaction_sync(&mut tx)?;
    let v = signature.v().y_parity_byte() as u64 + 27;
    let signature = EthSignature::from_signature_and_parity(*signature.inner(), v)?;

    Ok(req(tx.into_signed(signature)))
}

#[tokio::test]
#[serial]
async fn deploy_contract() -> anyhow::Result<()> {
    println!("started deploy_contract");
    let ExecutorTestEnvironment {
        test_ctx: mut env,
        test_kp: kp,
        executor,
        ..
    } = ExecutorTestEnvironment::start().await?;

    let code = Contract::read("tests/fixtures/hello_world")?;
    let mut tx = code.deploy_tx();
    let signature = kp.eth.sign_transaction_sync(&mut tx)?;
    let tx = tx.into_signed(signature);
    executor.handle_transaction(req(tx), None).await?;

    let _ = env.banks_client.get_account(FST_HOLDER_KEY).await?;
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

#[tokio::test]
#[serial]
async fn holder_detection() -> anyhow::Result<()> {
    println!("started holder_detection");
    let ExecutorTestEnvironment {
        test_ctx: mut env,
        test_kp: kp,
        executor,
        ..
    } = ExecutorTestEnvironment::start_with_program("evm_loader-1.14.5.so").await?;

    // TODO: Verify
    const CODE: &str = "608060405234801561001057600080fd5b50610315806100206000\
        396000f3fe608060405234801561001057600080fd5b50600436106100365760003560\
        e01c806353d91c351461003b578063fd543de814610064575b600080fd5b61004e6100\
        4936600461020d565b610079565b60405161005b9190610226565b60405180910390f3\
        5b61007761007236600461020d565b610113565b005b60006020819052908152604090\
        2080546100929061027b565b80601f0160208091040260200160405190810160405280\
        9291908181526020018280546100be9061027b565b801561010b5780601f106100e057\
        61010080835404028352916020019161010b565b820191906000526020600020905b81\
        54815290600101906020018083116100ee57829003601f168201915b50505050508156\
        5b60005b8181101561017057604080518082018252600d81526c557064617465645374\
        72696e6760981b602080830191825260008581529081905292909220905161015d9290\
        610174565b5080610168816102b6565b915050610116565b5050565b82805461018090\
        61027b565b90600052602060002090601f0160209004810192826101a2576000855561\
        01e8565b82601f106101bb57805160ff19168380011785556101e8565b828001600101\
        855582156101e8579182015b828111156101e857825182559160200191906001019061\
        01cd565b506101f49291506101f8565b5090565b5b808211156101f457600081556001\
        016101f9565b60006020828403121561021f57600080fd5b5035919050565b60006020\
        8083528351808285015260005b81811015610253578581018301518582016040015282\
        01610237565b81811115610265576000604083870101525b50601f01601f1916929092\
        016040019392505050565b600181811c9082168061028f57607f821691505b60208210\
        8114156102b057634e487b7160e01b600052602260045260246000fd5b50919050565b\
        60006000198214156102d857634e487b7160e01b600052601160045260246000fd5b50\
        6001019056fea26469706673582212204971630b4a2d997b5dc4d91cf3f9d7176f281f\
        3ba46831b9adb66e0d4c452a5164736f6c634300080c0033";
    let mut tx = TxLegacy {
        nonce: 0,
        gas_price: 1,
        gas_limit: 40_000_000,
        to: TxKind::Create,
        value: eth_to_wei(0).to_reth(),
        input: hex::decode(CODE).unwrap().into(),
        chain_id: Some(CHAIN_ID),
    };

    let signature = kp.eth.sign_transaction_sync(&mut tx)?;
    let tx = tx.into_signed(signature);
    executor.handle_transaction(req(tx), None).await?;

    let _ = env.banks_client.get_account(FST_HOLDER_KEY).await?;
    let txs = executor.join_current_transactions().await;
    assert!(txs.len() > 1);

    Ok(())
}

#[tokio::test]
#[serial]
async fn recover_holder() -> anyhow::Result<()> {
    println!("started recover_holder");
    let ExecutorTestEnvironment {
        test_ctx: mut env,
        test_kp: kp,
        executor,
        neon_api,
        solana_api,
        ..
    } = ExecutorTestEnvironment::start().await?;

    let code = Contract::read("tests/fixtures/hello_world")?;
    let mut tx = code.deploy_tx();
    let signature = kp.eth.sign_transaction_sync(&mut tx)?;
    let tx = tx.into_signed(signature);
    executor.handle_transaction(req(tx), None).await?;

    let _ = env.banks_client.get_account(FST_HOLDER_KEY).await?;
    let txs = executor.stop_after(3 /* Create + 2 Writes */).await;
    assert_eq!(txs.len(), 3);

    let account = env.banks_client.get_account(FST_HOLDER_KEY).await?.unwrap();
    assert_eq!(account.data[0], TAG_HOLDER); // We haven't started yet

    let operator = Operator::read_from_file("tests/keys/operator.json").map(Arc::new)?;
    let (executor, _task) = Executor::builder()
        .neon_pubkey(NEON_KEY)
        .operator(operator.clone())
        .neon_api(neon_api.clone())
        .solana_api(solana_api.clone())
        .init_operator_balance(false)
        .max_holders(MAX_HOLDERS)
        .holder_size(HOLDER_SIZE)
        .prepare()
        .start()
        .await?;
    let txs = executor.join_current_transactions().await;
    assert_eq!(txs.len(), 1);

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

#[tokio::test]
#[serial]
async fn iterations() -> anyhow::Result<()> {
    println!("started iterations");
    let ExecutorTestEnvironment {
        test_ctx: mut env,
        test_kp: kp,
        executor,
        ..
    } = ExecutorTestEnvironment::start().await?;

    let code = Contract::read("tests/fixtures/Counter")?;
    sol!(Counter, "tests/fixtures/Counter.abi");
    let call = Counter::moreInstructionCall {
        x: U256::from(0),
        y: U256::from(1000),
    }
    .abi_encode();

    let mut tx = code.deploy_tx();
    let signature = kp.eth.sign_transaction_sync(&mut tx)?;
    let tx = tx.into_signed(signature);
    // HACK: Fixes random AccountInUse error
    let _ = env.banks_client.get_account(FST_HOLDER_KEY).await?;
    executor.handle_transaction(req(tx), None).await?;

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

    let mut tx = TxLegacy {
        nonce: 1,
        gas_price: 2,
        gas_limit: u64::MAX.into(),
        to: TxKind::Call(contract_address.0.into()),
        value: eth_to_wei(0).to_reth(),
        input: call.into(),
        chain_id: Some(CHAIN_ID),
    };
    let signature = kp.eth.sign_transaction_sync(&mut tx)?;
    let tx = tx.into_signed(signature);

    executor.handle_transaction(req(tx), None).await?;
    let txs = executor.join_current_transactions().await;
    assert!(txs.len() > 1);

    Ok(())
}

#[tokio::test]
#[serial]
async fn recover_state() -> anyhow::Result<()> {
    println!("started recover_state");
    let ExecutorTestEnvironment {
        test_ctx: mut env,
        test_kp: kp,
        executor,
        neon_api,
        solana_api,
        ..
    } = ExecutorTestEnvironment::start().await?;

    let code = Contract::read("tests/fixtures/Counter")?;
    sol!(Counter, "tests/fixtures/Counter.abi");
    let call = Counter::moreInstructionCall {
        x: U256::from(0),
        y: U256::from(1000),
    }
    .abi_encode();

    let mut tx = code.deploy_tx();
    let signature = kp.eth.sign_transaction_sync(&mut tx)?;
    let tx = tx.into_signed(signature);
    // HACK: Fixes random AccountInUse error
    let _ = env.banks_client.get_account(FST_HOLDER_KEY).await?;
    executor.handle_transaction(req(tx), None).await?;

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

    let mut tx = TxLegacy {
        nonce: 1,
        gas_price: 2,
        gas_limit: u64::MAX.into(),
        to: TxKind::Call(contract_address.0.into()),
        value: eth_to_wei(0).to_reth(),
        input: call.into(),
        chain_id: Some(CHAIN_ID),
    };
    let signature = kp.eth.sign_transaction_sync(&mut tx)?;
    let tx = tx.into_signed(signature);

    executor.handle_transaction(req(tx), None).await?;
    let txs = executor.stop_after(2).await;
    assert!(txs.len() > 1);
    let account = env.banks_client.get_account(FST_HOLDER_KEY).await?.unwrap();
    assert_eq!(account.data[0], TAG_STATE); // We started

    let operator = Operator::read_from_file("tests/keys/operator.json").map(Arc::new)?;
    let (executor, _task) = Executor::builder()
        .neon_pubkey(NEON_KEY)
        .operator(operator.clone())
        .neon_api(neon_api.clone())
        .solana_api(solana_api.clone())
        .init_operator_balance(false)
        .max_holders(MAX_HOLDERS)
        .holder_size(HOLDER_SIZE)
        .prepare()
        .start()
        .await?;
    let txs = executor.join_current_transactions().await;
    assert!(txs.len() > 1);
    let account = env.banks_client.get_account(FST_HOLDER_KEY).await?.unwrap();
    assert_eq!(account.data[0], TAG_STATE_FINALIZED); // We finished

    Ok(())
}

#[tokio::test]
#[ignore] // TODO: Include this when solana crates are updated
async fn alt() -> anyhow::Result<()> {
    println!("started alt");
    let ExecutorTestEnvironment {
        test_ctx: mut env,
        test_kp: kp,
        executor,
        ..
    } = ExecutorTestEnvironment::start().await?;

    let mut code = Contract::read("tests/fixtures/ALT")?;
    sol!(Alt, "tests/fixtures/ALT.abi");
    let deploy_params = Alt::constructorCall {
        _count: U256::from(35),
    }
    .abi_encode();
    code.init.extend(deploy_params);
    let call = Alt::fillCall { N: U256::from(8) }.abi_encode();

    let mut tx = code.deploy_tx();
    let signature = kp.eth.sign_transaction_sync(&mut tx)?;
    let tx = tx.into_signed(signature);
    executor.handle_transaction(req(tx), None).await?;

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
    env.warp_to_slot(10)?;

    let mut tx = TxLegacy {
        nonce: 1,
        gas_price: 2,
        gas_limit: u64::MAX.into(),
        to: TxKind::Call(contract_address.0.into()),
        value: eth_to_wei(0).to_reth(),
        input: call.into(),
        chain_id: Some(CHAIN_ID),
    };
    let signature = kp.eth.sign_transaction_sync(&mut tx)?;
    let tx = tx.into_signed(signature);

    executor.handle_transaction(req(tx), None).await?;
    let txs = executor.join_current_transactions().await;
    assert!(txs.len() > 1);

    Ok(())
}

#[tokio::test]
#[serial]
async fn sol_call() -> anyhow::Result<()> {
    println!("started sol_call");
    let ExecutorTestEnvironment {
        test_ctx: mut env,
        test_kp: kp,
        executor,
        ..
    } = ExecutorTestEnvironment::start().await?;

    let code = Contract::read("tests/fixtures/Test")?;
    let mut tx = code.deploy_tx();
    let signature = kp.eth.sign_transaction_sync(&mut tx)?;
    let tx = tx.into_signed(signature);
    executor.handle_transaction(req(tx), None).await?;

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

// NOTE: Neon program gets cached and setting a new code account does not affect the runtime cache.
//     : `warp_to_slot` currently leads to inresponsive runtime and panics in BanksClient requests.
#[tokio::test]
#[ignore]
async fn reload_config() -> Result<()> {
    println!("started reload_config");
    let ExecutorTestEnvironment {
        rpc,
        test_kp: kp1,
        executor,
        test_ctx: mut ctx,
        ..
    } = ExecutorTestEnvironment::start().await?;

    let payer = ctx.payer.insecure_clone();
    let kp2 = Wallet::new();
    let address2 = kp2.address();

    // Transfer
    do_transfer(&executor, &rpc, &kp1, address2, 900, 2).await?;

    let code_file = find_file("evm_loader-1.14.5.so").context("cannot find old evm_loader")?;
    let data = read_file(code_file);

    // Update program
    let buffer = Keypair::new();
    let ixs = bpf_loader_upgradeable::create_buffer(
        &payer.pubkey(),
        &buffer.pubkey(),
        &payer.pubkey(),
        ctx.banks_client.get_rent().await?.minimum_balance(
            bpf_loader_upgradeable::UpgradeableLoaderState::size_of_buffer(data.len()),
        ),
        data.len(),
    )?;
    ctx.send_instructions(&ixs, &[&payer, &buffer]).await?;

    const CHUNK_SIZE: usize = 1024;
    for (idx, chunk) in data.chunks(CHUNK_SIZE).enumerate() {
        let ix = bpf_loader_upgradeable::write(
            &buffer.pubkey(),
            &payer.pubkey(),
            (idx * CHUNK_SIZE) as u32,
            chunk.to_vec(),
        );
        ctx.send_instructions(&[ix], &[&payer]).await?;
    }

    let ix = bpf_loader_upgradeable::upgrade(
        &NEON_KEY,
        &buffer.pubkey(),
        &payer.pubkey(),
        &payer.pubkey(),
    );
    ctx.send_instructions(&[ix], &[&payer]).await?;
    ctx.warp_to_slot(2)?;

    executor.reload_config().await?;

    let kp3 = Wallet::new();
    let address3 = kp3.address();

    // Transfer
    do_transfer(&executor, &rpc, &kp2, address3, 800, 1).await?;

    Ok(())
}

/// Tests that holders get reused
#[tokio::test]
#[serial]
async fn parallel_transfers() -> Result<()> {
    println!("started parallel_transfers");
    const NUM_TRANSFERS: usize = 100;
    let ExecutorTestEnvironment {
        rpc,
        // test_kp,
        executor,
        test_ctx: mut ctx,
        ..
    } = ExecutorTestEnvironment::start().await?;

    let mut pairs = Vec::new();

    for _ in 0..NUM_TRANSFERS {
        let kp1 = Wallet::new();
        let address1 = kp1.address();
        let kp2 = Wallet::new();
        let address2 = kp2.address();

        mint_and_deposit_to_neon(&mut ctx, &kp1, 1_000).await?;

        let balance = get_balances(&rpc, &[address1, address2]).await?;
        assert_eq!(balance[0].balance, eth_to_wei(1_000));
        assert_eq!(balance[1].balance, eth_to_wei(0));
        pairs.push((kp1, address2));
    }

    // Transfer
    for (kp, addr) in &pairs {
        executor
            .handle_transaction(build_transfer_no_chain_id(kp, *addr)?, None)
            .await?;
    }

    // HACK: Fixes random AccountInUse error
    let _ = ctx.banks_client.get_account(FST_HOLDER_KEY).await?;
    let txs = executor.join_current_transactions().await;
    // Each transfer without chain id results into at least 4 txs: 1 write to holder and 3 steps.
    // First `MAX_HOLDERS` transfers will also create holder accounts which add 1 tx.
    assert_eq!(txs.len(), 4 * NUM_TRANSFERS + 10);

    for (kp, addr) in &pairs {
        let balance = get_balances(&rpc, &[kp.address(), *addr]).await?;
        assert!(balance[0].balance < eth_to_wei(100));
        assert_eq!(balance[1].balance, eth_to_wei(900));
    }
    Ok(())
}

async fn find_holder(idx: u8, solana_api: SolanaApi, operator: &Pubkey) -> Result<Option<Account>> {
    let seed = format!("holder{}", idx);
    let pubkey =
        Pubkey::create_with_seed(operator, &seed, &NEON_KEY).expect("create with seed failed");
    let Some(account) = solana_api.get_account(&pubkey).await? else {
        return Ok(None);
    };
    Ok(Some(account))
}

#[tokio::test]
#[serial]
async fn holder_recreate() -> Result<()> {
    println!("started holder_recreate");
    let ExecutorTestEnvironment {
        neon_api,
        solana_api,
        ..
    } = ExecutorTestEnvironment::start().await?;

    let operator = Operator::read_from_file("tests/keys/operator.json").map(Arc::new)?;
    let (executor, _task) = Executor::builder()
        .neon_pubkey(NEON_KEY)
        .operator(operator.clone())
        .neon_api(neon_api.clone())
        .solana_api(solana_api.clone())
        .init_operator_balance(false)
        .max_holders(1)
        .holder_size(HOLDER_SIZE)
        .init_holders(true)
        .prepare()
        .start()
        .await?;
    executor.join_current_transactions().await;

    let holder = find_holder(0, solana_api.clone(), &operator.pubkey()).await?;
    let holder_len = holder.unwrap().data.len();
    assert_eq!(holder_len, HOLDER_SIZE);

    let (executor, _task) = Executor::builder()
        .neon_pubkey(NEON_KEY)
        .operator(operator.clone())
        .neon_api(neon_api.clone())
        .solana_api(solana_api.clone())
        .init_operator_balance(false)
        .max_holders(1)
        .holder_size(HOLDER_SIZE + 100)
        .init_holders(true)
        .prepare()
        .start()
        .await?;
    executor.join_current_transactions().await;

    let holder = find_holder(0, solana_api.clone(), &operator.pubkey()).await?;
    let holder_len = holder.unwrap().data.len();
    assert_eq!(holder_len, HOLDER_SIZE + 100);

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
            gas_limit: u64::MAX.into(),
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

fn req(tx: impl Into<TxEnvelope>) -> ExecuteRequest {
    ExecuteRequest::new(tx.into(), CHAIN_ID)
}
