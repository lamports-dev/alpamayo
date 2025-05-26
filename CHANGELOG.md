# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note:** Version 0 of Semantic Versioning is handled differently from version 1 and above.
The minor version will be incremented upon a breaking change and the patch version will be incremented for features.

## [Unreleased]

### Fixes

### Features

### Breaking

## [0.8.7] - 2025-05-26

## Fixes

- rpc: check first available slot on startup ([#52](https://github.com/lamports-dev/alpamayo/pull/52))

## [0.8.6] - 2025-05-25

## Fixes

- storage: fix dead slots in memory storage ([#51](https://github.com/lamports-dev/alpamayo/pull/51))

## [0.8.5] - 2025-05-25

## Fixes

- storage: fix dead slots in memory storage ([#50](https://github.com/lamports-dev/alpamayo/pull/50))

## [0.8.4] - 2025-05-24

## Fixes

- storage: fix dead slots in memory storage ([#49](https://github.com/lamports-dev/alpamayo/pull/49))

## [0.8.3] - 2025-05-23

## Fixes

- storage: fix backfilling for read layer ([#48](https://github.com/lamports-dev/alpamayo/pull/48))

## [0.8.2] - 2025-05-22

## Fixes

- storage: fix slots index loading ([#47](https://github.com/lamports-dev/alpamayo/pull/47))

## [0.8.1] - 2025-05-22

### Fixes

- storage: add height check for new blocks ([#45](https://github.com/lamports-dev/alpamayo/pull/45))
- rpc: use `-32009` for missed slot too ([#46](https://github.com/lamports-dev/alpamayo/pull/46))

## [0.8.0] - 2025-05-18

### Features

- storage: add read thread options ([#44](https://github.com/lamports-dev/alpamayo/pull/44))

## [0.7.2] - 2025-05-15

### Fixes

- storage: fix backfilling finish condition ([#42](https://github.com/lamports-dev/alpamayo/pull/42))

## [0.7.1] - 2025-05-13

### Fixes

- storage: use rocksdb `set_max_background_jobs` ([#41](https://github.com/lamports-dev/alpamayo/pull/41))

## [0.7.0] - 2025-05-10

### Features

- rpc: serialize to `Vec<u8>` instead of `serde_json::Value` ([#38](https://github.com/lamports-dev/alpamayo/pull/38))
- rpc: remove get_block concurrency ([#39](https://github.com/lamports-dev/alpamayo/pull/39))
- storage: impl backfilling ([#40](https://github.com/lamports-dev/alpamayo/pull/40))

## [0.6.1] - 2025-04-30

### Fixes

- rpc: ignore send result in read threads ([#37](https://github.com/lamports-dev/alpamayo/pull/37))

## [0.6.0] - 2025-04-29

### Features

- rpc: add `getInflationReward` ([#36](https://github.com/lamports-dev/alpamayo/pull/36))

## [0.5.0] - 2025-04-26

### Features

- rpc: add smart cache ([#34](https://github.com/lamports-dev/alpamayo/pull/34))
- source: add gRPC reconnect ([#35](https://github.com/lamports-dev/alpamayo/pull/35))

## x[0.4.0] - 2025-04-22

### Features

- rpc: add `getFirstAvailableBlock` support ([#31](https://github.com/lamports-dev/alpamayo/pull/31))

## [0.3.0] - 2025-04-21

### Features

- bench: add received size ([#29](https://github.com/lamports-dev/alpamayo/pull/29))
- rpc: use jsonrpc feature from richat ([#30](https://github.com/lamports-dev/alpamayo/pull/30))

## [0.2.1] - 2025-04-17

### Fixes

- storage: fix catch-up on startup ([#28](https://github.com/lamports-dev/alpamayo/pull/28))

### Features

- rpc: support HTTP/GET `/version` ([#28](https://github.com/lamports-dev/alpamayo/pull/28))

## [0.2.0] - 2025-04-17

### Fixes

- storage: fix dead block removal ([#27](https://github.com/lamports-dev/alpamayo/pull/27))

### Features

- rpc: support rest for block and tx ([#26](https://github.com/lamports-dev/alpamayo/pull/26))

## [0.1.0] - 2025-04-14

### Fixes

- rpc: add `getSlot` to config ([#1](https://github.com/lamports-dev/alpamayo/pull/1))
- rpc: change upstream header to `x-bigtable: disabled` ([#5](https://github.com/lamports-dev/alpamayo/pull/5))
- rpc: use confirmed during sync ([#6](https://github.com/lamports-dev/alpamayo/pull/6))
- storage: remove transaction index on slot remove ([#8](https://github.com/lamports-dev/alpamayo/pull/8))
- storage: set confirmed/finalized on first stream messages ([#17](https://github.com/lamports-dev/alpamayo/pull/17))
- rpc: load recent blocks on startup ([#23](https://github.com/lamports-dev/alpamayo/pull/23))
- rpc: move `ready` endpoint to metrics server ([#24](https://github.com/lamports-dev/alpamayo/pull/24))

### Features

- source: add fast ConfirmedBlock serialization ([#3](https://github.com/lamports-dev/alpamayo/pull/3))
- storage: add multiple readers ([#4](https://github.com/lamports-dev/alpamayo/pull/4))
- storage: support getTransaction ([#7](https://github.com/lamports-dev/alpamayo/pull/7))
- storage: support getBlockHeight ([#9](https://github.com/lamports-dev/alpamayo/pull/9))
- storage: support getSignaturesForAddress ([#10](https://github.com/lamports-dev/alpamayo/pull/10))
- storage: split slot index ([#11](https://github.com/lamports-dev/alpamayo/pull/11))
- storage: support compression for indexes ([#12](https://github.com/lamports-dev/alpamayo/pull/12))
- storage: store err in tx-index ([#13](https://github.com/lamports-dev/alpamayo/pull/13))
- rpc: support getVersion ([#14](https://github.com/lamports-dev/alpamayo/pull/14))
- rpc: support getBlockTime ([#15](https://github.com/lamports-dev/alpamayo/pull/15))
- rpc: support getBlocks / getBlocksWithLimit ([#16](https://github.com/lamports-dev/alpamayo/pull/16))
- rpc: custom gSFA limit ([#18](https://github.com/lamports-dev/alpamayo/pull/18))
- rpc: support gSS ([#19](https://github.com/lamports-dev/alpamayo/pull/19))
- rpc: support getLatestBlockhash ([#20](https://github.com/lamports-dev/alpamayo/pull/20))
- rpc: support isBlockhashValid ([#21](https://github.com/lamports-dev/alpamayo/pull/21))
- rpc: support getRecentPrioritizationFees ([#22](https://github.com/lamports-dev/alpamayo/pull/22))
- metrics: move to metrics.rs ([#25](https://github.com/lamports-dev/alpamayo/pull/25))
