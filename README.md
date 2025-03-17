# alpamayo

Lightweight drop-in replacement for the Solana RPC stack, designed for frozen data (blocks, transactions, etc.).

## Sponsored by

## Licensing

The project is dual-licensed: the open-source version uses the `AGPL-3.0-only` license, while the private version is licensed under a [commercial license](https://gist.github.com/fanatid/13f3d557c89efbf3b0c32a2d07345444#file-commercial-license-md).

## Alpamayo support and enterprise version

Please use issues only for bug or feature-related topics. If you're having trouble running alpamayo or need insights about the configuration, please drop your question in the Telegram chat: [https://t.me/lamportsdev](https://t.me/lamportsdev)

In addition to the open-source version there also private version with **[Prometheus](https://prometheus.io/) metrics** and ability to **stream raw blocks in protobuf** (with full access to code). To get more info please send email to: [customers@lamports.dev](mailto:customers@lamports.dev)

<details>
<summary>metrics example (click to toggle):</summary>

```
TODO
```
</details>

## Supported methods

### Solana Rpc methods:

- [x] `getBlock`
- [ ] `getBlockHeight`
- [ ] `getBlocks`
- [ ] `getBlockTime`
- [ ] `getLatestBlockhash`
- [ ] `getRecentPrioritizationFees`
- [ ] `getSignatureStatuses`
- [x] `getSlot`
- [ ] `getTransaction`
- [ ] `getVersion`
- [ ] `isBlockhashValid`

### Extra methods:

- [ ] `getBlockProtobuf`
- [ ] `getVersionAlpamayo`

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
