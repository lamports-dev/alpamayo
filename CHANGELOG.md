# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note:** Version 0 of Semantic Versioning is handled differently from version 1 and above.
The minor version will be incremented upon a breaking change and the patch version will be incremented for features.

## [Unreleased]

### Fixes

- rpc: add `getSlot` to config ([#1](https://github.com/lamports-dev/alpamayo/pull/1))
- rpc: change upstream header to `x-bigtable: disabled` ([#5](https://github.com/lamports-dev/alpamayo/pull/5))
- rpc: use confirmed during sync ([#6](https://github.com/lamports-dev/alpamayo/pull/6))
- storage: remove transaction index on slot remove ([#8](https://github.com/lamports-dev/alpamayo/pull/8))

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

### Breaking
