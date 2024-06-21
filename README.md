Proxy/Indexer Service - Completed Development Summary

Based on the initial requirements and requirements for Milestone 1, we have successfully developed and implemented the following components as part of the comprehensive service to provide an Ethereum-like RPC interface for Ethereum-like clients interacting with smart contracts deployed using Neon EVM on the Solana network. This partial development is a step towards enabling the integration of Ethereum-like clients with Neon EVM, expanding interaction capabilities within the blockchain ecosystem.

### Services Developed:

#### Development of Ethereum-like RPC Endpoint with Support for a Specific Set of Methods and Integrations:

-   Provision of an RPC interface compatible with Ethereum and Indexer DB.

-   Interaction with a database to access indexed information about transactions in Neon EVM.

#### Indexer Service:

-   Collection and indexing of information about all transactions in the Neon EVM network, not limited to data from the current operator.

-   Storage of indexed data in the database.

-   Processing multiple Solana transactions associated with Neon transactions.

-   The Indexer saves information about Ethereum transactions only when all the corresponding Neon transactions on the Solana network have been finalized.

-   Exchange of information between Neon EVM and Indexer occurs through the reception of information from Solana.

For indexing and retrieving data from Solana, a specially developed unique approach was applied, differing from the Python MVP while fully meeting the requirements.\
Milestone 1 Achievements:

1.  Indexing of Completed Neon Transactions:

-   Indexing all types of Neon transactions, including non-iterative and iterative executions from both Instruction Data and Holder Account.

-   Support for iterative execution from Holder Account transactions before EIP-155.

-   Note: The indexer does not index Address Lookup Table (ALT) transactions as per the requirements. Instead, accounts loaded through ALT are obtained from `loaded_addresses` field from Solana RPC.

2.  Development of Ethereum-like RPC Endpoint:

-   Implemented with support for a specific set of methods and integrations:

-   Retrieve a transaction by hash - eth_getTransactionByHash

-   Retrieve a transaction receipt - eth_getTransactionReceipt

-   Retrieve logs of transaction execution - eth_getLogs

-   Retrieve a block by hash - eth_getBlockByHash

-   Retrieve a block by number - eth_getBlockByNumber

-   Retrieve the last block number - eth_blockNumber

### Database Enhancements:

-   Indexer DB:

-   Additional rules on data types and restrictions were added.

-   An additional table neon_holder_log was introduced to better manage and store transaction data.

### Architectural Implementation:

-   The RPC and Indexer (including IndexerDB) were implemented as two separate services, ensuring modularity and maintainability.

    Full Service Diagram:

![](https://lh7-us.googleusercontent.com/docsz/AD_4nXf8B-31zslp0Htws7sOxFISe9wxfdzL7x2uKQ-QBcbHf-oRB2Sz99nDUo5UI1GToMjv6P3ruifkAsdJ36qAW9qdGnXRIzkr5y_bWdurWxTLnh2U29HJMNINCqsAxnIvravie30CDsFMHPUQshDG8hICZ0c?key=viXkXl20VcRQCRR3NYXrag)\
Data Flow:\
The data flow from the Client to RPC to Indexer DB to Indexer has been established, with the Indexer constantly communicating with Solana to fetch transactions related to Neon EVM and store them in the Indexer DB.

### Build
To build local a docker image of the neon-proxy with the name `your-organization/neon-proxy:latest`, set-up environment variables and execute
```
export DOCKERHUB_ORG_NAME=your-organization
export NEON_PROXY_REPO_NAME=neon-proxy
export NEON_PROXY_TAG=latest
./build-docker.sh
```

### Run
To run the built image on Solana Devnet, set-up environment variables and execute
```
export DOCKERHUB_ORG_NAME=your-organization
export NEON_PROXY_REPO_NAME=neon-proxy
export NEON_PROXY_TAG=latest
export NEON_EVM_ADDRESS=eeLSJgWzzxrqKv1UxtRVVH8FX3qCQWUs9QuAjJpETGU
export SOLANA_URL=http://sol-devnet-01.tt-int.net:8899
docker-compose up -d
```

To run the built image on local Solana validator with Neon EVM, set-up environment variables and execute
```
export DOCKERHUB_ORG_NAME=your-organization
export NEON_PROXY_REPO_NAME=neon-proxy
export NEON_PROXY_TAG=latest
export NEON_EVM_ADDRESS=53DfF883gyixYNXnM7s5xhdeyV8mVk9T4i2hGV9vG9io
export NEON_EVM_COMMIT=67d1bd0ef32e5ab6f96393419ae25053080d2a9e
docker-compose -f docker-compose-evm.yml up -d
```