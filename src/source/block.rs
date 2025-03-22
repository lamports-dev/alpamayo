use {
    prost::{
        DecodeError, Message,
        bytes::{Buf, BufMut},
        encoding::{
            self, DecodeContext, WireType, encode_key, encode_varint, encoded_len_varint, key_len,
        },
    },
    solana_sdk::clock::{Slot, UnixTimestamp},
    solana_storage_proto::convert::generated,
    solana_transaction_status::{ConfirmedBlock, Reward, RewardType},
    std::ops::Deref,
};

#[derive(Debug)]
pub struct ConfirmedBlockWithBinary {
    pub parent_slot: Slot,
    pub block_time: Option<UnixTimestamp>,
    buffer: Vec<u8>,
}

impl ConfirmedBlockWithBinary {
    pub fn new(block: ConfirmedBlock, transactions_proto: Option<Vec<Vec<u8>>>) -> Self {
        let transactions_proto = transactions_proto.unwrap_or_else(|| {
            block
                .transactions
                .iter()
                .cloned()
                .map(|tx| generated::ConfirmedTransaction::from(tx).encode_to_vec())
                .collect()
        });

        let parent_slot = block.parent_slot;
        let block_time = block.block_time;

        let buffer = ConfirmedBlockProtoRef {
            previous_blockhash: &block.previous_blockhash,
            blockhash: &block.blockhash,
            parent_slot: block.parent_slot,
            transactions: &transactions_proto,
            rewards: &block.rewards,
            block_time: block
                .block_time
                .map(|timestamp| generated::UnixTimestamp { timestamp }),
            block_height: block
                .block_height
                .map(|block_height| generated::BlockHeight { block_height }),
            num_partitions: block
                .num_partitions
                .map(|num_partitions| generated::NumPartitions { num_partitions }),
        }
        .encode_to_vec();

        Self {
            parent_slot,
            block_time,
            buffer,
        }
    }

    pub fn take_buffer(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buffer)
    }
}

#[derive(Debug)]
struct ConfirmedBlockProtoRef<'a> {
    previous_blockhash: &'a String,
    blockhash: &'a String,
    parent_slot: Slot,
    transactions: &'a [Vec<u8>],
    rewards: &'a [Reward],
    block_time: Option<generated::UnixTimestamp>,
    block_height: Option<generated::BlockHeight>,
    num_partitions: Option<generated::NumPartitions>,
}

impl Message for ConfirmedBlockProtoRef<'_> {
    fn encode_raw<B>(&self, buf: &mut B)
    where
        B: BufMut,
        Self: Sized,
    {
        if !self.previous_blockhash.is_empty() {
            bytes_encode(1, self.previous_blockhash.as_ref(), buf);
        }
        if !self.blockhash.is_empty() {
            bytes_encode(2, self.blockhash.as_ref(), buf);
        }
        if self.parent_slot != 0 {
            encoding::uint64::encode(3, &self.parent_slot, buf);
        }
        for tx in self.transactions {
            encode_key(4, WireType::LengthDelimited, buf);
            encode_varint(tx.len() as u64, buf);
            buf.put_slice(tx);
        }
        for reward in self.rewards {
            encoding::message::encode(5, &RewardWrapper(reward), buf);
        }
        if let Some(block_time) = &self.block_time {
            encoding::message::encode(6, block_time, buf);
        }
        if let Some(block_height) = &self.block_height {
            encoding::message::encode(7, block_height, buf);
        }
        if let Some(num_partitions) = &self.num_partitions {
            encoding::message::encode(8, num_partitions, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        (if !self.previous_blockhash.is_empty() {
            bytes_encoded_len(1, self.previous_blockhash.as_ref())
        } else {
            0
        }) + if !self.blockhash.is_empty() {
            bytes_encoded_len(2, self.blockhash.as_ref())
        } else {
            0
        } + if self.parent_slot != 0 {
            encoding::uint64::encoded_len(3, &self.parent_slot)
        } else {
            0
        } + (key_len(4u32) * self.transactions.len()
            + self
                .transactions
                .iter()
                .map(|slice| slice.len())
                .map(|len| len + encoded_len_varint(len as u64))
                .sum::<usize>())
            + (key_len(5u32) * self.rewards.len()
                + self
                    .rewards
                    .iter()
                    .map(|reward| RewardWrapper(reward).encoded_len())
                    .map(|len| len + encoded_len_varint(len as u64))
                    .sum::<usize>())
            + if let Some(block_time) = &self.block_time {
                encoding::message::encoded_len(6, block_time)
            } else {
                0
            }
            + if let Some(block_height) = &self.block_height {
                encoding::message::encoded_len(7, block_height)
            } else {
                0
            }
            + if let Some(num_partitions) = &self.num_partitions {
                encoding::message::encoded_len(8, num_partitions)
            } else {
                0
            }
    }

    fn clear(&mut self) {
        unimplemented!()
    }

    fn merge_field<B>(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut B,
        _ctx: DecodeContext,
    ) -> Result<(), DecodeError>
    where
        B: Buf,
        Self: Sized,
    {
        unimplemented!()
    }
}

#[derive(Debug)]
struct RewardWrapper<'a>(&'a Reward);

impl Deref for RewardWrapper<'_> {
    type Target = Reward;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl Message for RewardWrapper<'_> {
    fn encode_raw<B>(&self, buf: &mut B)
    where
        B: BufMut,
        Self: Sized,
    {
        if !self.pubkey.is_empty() {
            encoding::string::encode(1, &self.pubkey, buf);
        }
        if self.lamports != 0 {
            encoding::int64::encode(2, &self.lamports, buf);
        }
        if self.post_balance != 0 {
            encoding::uint64::encode(3, &self.post_balance, buf);
        }
        if self.reward_type.is_some() {
            encoding::int32::encode(4, &reward_type_as_i32(self.reward_type), buf);
        }
        if let Some(commission) = self.commission {
            bytes_encode(5, u8_to_static_str(commission).as_ref(), buf);
        }
    }

    fn encoded_len(&self) -> usize {
        (if !self.pubkey.is_empty() {
            encoding::string::encoded_len(1, &self.pubkey)
        } else {
            0
        }) + if self.lamports != 0 {
            encoding::int64::encoded_len(2, &self.lamports)
        } else {
            0
        } + if self.post_balance != 0 {
            encoding::uint64::encoded_len(3, &self.post_balance)
        } else {
            0
        } + if self.reward_type.is_some() {
            encoding::int32::encoded_len(4, &reward_type_as_i32(self.reward_type))
        } else {
            0
        } + self.commission.map_or(0, |commission| {
            bytes_encoded_len(5, u8_to_static_str(commission).as_ref())
        })
    }

    fn clear(&mut self) {
        unimplemented!()
    }

    fn merge_field<B>(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut B,
        _ctx: DecodeContext,
    ) -> Result<(), DecodeError>
    where
        B: Buf,
        Self: Sized,
    {
        unimplemented!()
    }
}

pub const fn reward_type_as_i32(reward_type: Option<RewardType>) -> i32 {
    match reward_type {
        None => 0,
        Some(RewardType::Fee) => 1,
        Some(RewardType::Rent) => 2,
        Some(RewardType::Staking) => 3,
        Some(RewardType::Voting) => 4,
    }
}

#[inline]
pub fn bytes_encode(tag: u32, value: &[u8], buf: &mut impl BufMut) {
    encode_key(tag, WireType::LengthDelimited, buf);
    encode_varint(value.len() as u64, buf);
    buf.put(value)
}

#[inline]
pub fn bytes_encoded_len(tag: u32, value: &[u8]) -> usize {
    key_len(tag) + encoded_len_varint(value.len() as u64) + value.len()
}

const NUM_STRINGS: [&str; 256] = [
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16",
    "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32",
    "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48",
    "49", "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "60", "61", "62", "63", "64",
    "65", "66", "67", "68", "69", "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "80",
    "81", "82", "83", "84", "85", "86", "87", "88", "89", "90", "91", "92", "93", "94", "95", "96",
    "97", "98", "99", "100", "101", "102", "103", "104", "105", "106", "107", "108", "109", "110",
    "111", "112", "113", "114", "115", "116", "117", "118", "119", "120", "121", "122", "123",
    "124", "125", "126", "127", "128", "129", "130", "131", "132", "133", "134", "135", "136",
    "137", "138", "139", "140", "141", "142", "143", "144", "145", "146", "147", "148", "149",
    "150", "151", "152", "153", "154", "155", "156", "157", "158", "159", "160", "161", "162",
    "163", "164", "165", "166", "167", "168", "169", "170", "171", "172", "173", "174", "175",
    "176", "177", "178", "179", "180", "181", "182", "183", "184", "185", "186", "187", "188",
    "189", "190", "191", "192", "193", "194", "195", "196", "197", "198", "199", "200", "201",
    "202", "203", "204", "205", "206", "207", "208", "209", "210", "211", "212", "213", "214",
    "215", "216", "217", "218", "219", "220", "221", "222", "223", "224", "225", "226", "227",
    "228", "229", "230", "231", "232", "233", "234", "235", "236", "237", "238", "239", "240",
    "241", "242", "243", "244", "245", "246", "247", "248", "249", "250", "251", "252", "253",
    "254", "255",
];

const fn u8_to_static_str(num: u8) -> &'static str {
    NUM_STRINGS[num as usize]
}
