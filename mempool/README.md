# Mempool Documentation

## Overview

The mempool is a transaction queue where submitted transactions await execution. It follows a set of rules to determine
how and when transactions are selected for execution.

Each chain ID has its own queue for executing transactions, and transactions are prioritized based on their gas price.
The transaction with the highest gas price is selected first. For each sender, only one transaction can be executed
at a time. Additionally, every transaction must have a nonce equal to the number of transactions already executed for
that sender on the specific chain (the first transaction has a nonce of zero).

The mempool implementation includes the following queues and structures:

1. A map that tracks all transactions for all chain IDs and senders.
2. For each chain ID, there is a pool of transaction (`chain pool`) that is sorted by gas price and holds transactions
   ready for execution. Each sender can only have one transaction in this queue, ensuring that no two transactions from
   the same sender are executed simultaneously.
3. Each sender has its own pool of transactions (`sender pool`) with queues for managing transactions scheduled
   for execution. These are divided into two categories:
    - **Pending queue**: Contains transactions starting with the one whose nonce equals the sender's current transaction
      count. Subsequent transactions must have consecutive nonces.
    - **Gapped queue**: Contains transactions that follow a gap in the sequence of nonces (if the first transaction
      in the queue does not match the sender's transaction count, or if there's a gap between transactions).

When a new transaction is added to the mempool, it is placed in either the pending or gapped queue. Once the sender's
transaction is ready for execution and there's an available spot in the `chain pool`, the transaction moves from the
`sender pool` to the `chain pool` for execution.

## Sender Pool States

1. **Idle**: No transaction from this pool is currently scheduled in the `chain pool`.
2. **Queued(nonce)**: A transaction from this `sender pool` with the specified `nonce` is waiting in the `chain pool`
   for execution. No new transactions can be added from this pool.
3. **Processing(nonce)**: A transaction from this pool is currently being executed. The mempool is waiting for
   the result, and no new transactions can be added to the `chain pool` until the current one completes.

When a transaction is selected from the `chain pool` for execution, the corresponding `sender pool` transitions to the
`Processing` state. If the transaction completes successfully, the pool returns to the `Idle` state and tries
to schedule the next transaction. If successful, it moves to `Queued`.

## Rules for Adding a Transaction to the Mempool

When a new transaction is added to the mempool, the following checks are made:

1. **Unique transaction hash**: The mempool ensures that there is no other transaction with the same hash.
2. **Known chain ID**: The transaction must be for a recognized chain ID.
3. **Processing state**: If the sender’s pool is in the `Processing` state and the transaction’s nonce matches
   the currently executing transaction’s nonce, the new transaction is rejected.
4. **Nonce validation**: The transaction's nonce must be greater than the sender's current transaction count.
5. **Replacing an existing transaction**: If a transaction with the same nonce already exists in the `sender pool`,
   the new transaction is only accepted if its gas price is higher. Otherwise, it is rejected.
6. **High watermark condition**: If the transaction count exceeds the high watermark for the `chain pool` and the new
   transaction creates a gap (i.e., its nonce is higher than the current transaction count), the transaction is only
   accepted if its gas price is higher than the lowest gas price of the gapped transactions in the pool. If there are no
   gapped transactions, the new transaction is rejected. If the `chain pool` capacity is exceeded and the new
   transaction
   does not create a gap (or if no gapped transactions exist), the transaction is accepted only if its gas price is
   higher
   than the lowest gas price in the pool.
7. **Capacity overflow**: If the number of transactions in the `chain pool` exceeds the capacity, the mempool purges
   excess transactions before adding the new one:
    - First, it removes gapped transactions, starting with the one with the lowest gas price.
    - Then, it removes pending transactions, starting with the one with the lowest gas price.
