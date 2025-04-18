# alpamayo

Lightweight drop-in replacement for the Solana RPC stack, designed for frozen data (blocks, transactions, etc.).

Please use issues only for reporting bugs or discussing feature-related topics. If you're having trouble running alpamayo or need insights about the configuration, please post your question in the Telegram group: [https://t.me/lamportsdev](https://t.me/lamportsdev)

## Sponsored by

## Supported methods

### Solana Rpc methods:

- [x] `getBlock`
- [x] `getBlockHeight`
- [x] `getBlocks`
- [x] `getBlocksWithLimit`
- [x] `getBlockTime`
- [x] `getLatestBlockhash`
- [x] `getRecentPrioritizationFees`
- [x] `getSignaturesForAddress`
- [x] `getSignatureStatuses`
- [x] `getSlot`
- [x] `getTransaction`
- [x] `getVersion`
- [x] `isBlockhashValid`

### Extra methods:

- [x] `/block/${slot}`
- [x] `/tx/${signature}`
- [x] `/version`

## Blueprint

```mermaid
flowchart LR
    subgraph source1 [**agave**]
        subgraph source1_geyser1 [Yellowstone-gRPC]
        end
        subgraph source_geyser2 [richat-plugin-agave]
        end
        subgraph source_rpc1 [RPC-Server]
        end
    end

    subgraph source2 [**richat**]
    end

    subgraph alpamayo1 [**alpamayo**]
        subgraph tokio1 [Tokio Runtime]
            tokio1_receiver(subscriber)
            tokio1_bank[(transactions)]
            tokio1_rpc(RPC-Client)
            tokio1_metrics(Metrics / Prometheus)
        end

        subgraph storage1 [Thread / Storage]
            storage1_processor(processor)
            storage1_processed[(processed blocks<br/>**memory**)]
            storage1_confirmed[(confirmed blocks<br/>**files**)]
        end

        subgraph rpc1 [Tokio Runtime]
            rpc1_http(RPC-Server)
        end

        subgraph workers1 [Thread Pool / RPC]
            workers1_th1(Worker 1)
            workers1_thn(Worker N)
        end

        alpamayo1_slots[(**current slots:**<br/>first stored<br/>finalized<br/>confirmed<br/>processed)]
    end

    client1(client)
    client2(client/monitoring)

    source1_geyser1 -.->|gRPC| tokio1_receiver
    tokio1_rpc --> source_rpc1
    source_geyser2 -->|Tcp / gRPC / Quic<br/>full stream| source2
    source2 -.->|gRPC| tokio1_receiver
    tokio1_receiver --> tokio1_bank
    tokio1_bank --> storage1_processor
    storage1_processor --> tokio1_rpc
    storage1_processor --> storage1_processed
    storage1_processor --> storage1_confirmed
    storage1_processor --> alpamayo1_slots
    rpc1_http --> storage1_processor
    rpc1_http --> workers1_th1
    rpc1_http --> workers1_thn
    rpc1_http --> source_rpc1
    rpc1_http --> alpamayo1_slots
    client1 --> rpc1_http
    client2 --> tokio1_metrics
```
