use std::cmp;
use std::cmp::Ordering;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};

/// Maximum physical clock drift allowed, in ms
const MAX_DRIFT_MS: u64 = 60_000;

#[derive(Serialize, Deserialize, Archive, Debug, Copy, Clone, Eq, PartialEq)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct HLCTimestamp {
    millis: u64,
    counter: u16,
    node: u32,
}

impl PartialOrd<Self> for HLCTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HLCTimestamp {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.millis, self.counter).cmp(&(other.millis, other.counter))
    }
}

impl HLCTimestamp {
    pub fn new(millis: u64, counter: u16, node: u32) -> Self {
        Self {
            millis,
            counter,
            node,
        }
    }

    #[inline]
    pub fn node(&self) -> u32 {
        self.node
    }

    #[inline]
    pub fn counter(&self) -> u16 {
        self.counter
    }

    #[inline]
    pub fn millis(&self) -> u64 {
        self.millis
    }
    /// Timestamp send. Generates a unique, monotonic timestamp suitable
    /// for transmission to another system in string format
    pub fn send(&mut self) -> Result<Self, TimestampError> {
        let ts = get_unix_timestamp_ms();

        let ts_old = self.millis;
        let c_old = self.counter;

        // Calculate the next logical time and counter
        // * ensure that the logical time never goes backward
        // * increment the counter if phys time does not advance
        let ts_new = cmp::max(ts_old, ts);

        if ts_new.saturating_sub(ts) > MAX_DRIFT_MS {
            return Err(TimestampError::ClockDrift);
        }

        let c_new = if ts_old == ts_new {
            c_old.checked_add(1).ok_or(TimestampError::Overflow)?
        } else {
            0
        };

        self.millis = ts_new;
        self.counter = c_new;

        Ok(*self)
    }

    /// Timestamp receive. Parses and merges a timestamp from a remote
    /// system with the local time-global uniqueness and monotonicity are
    /// preserved
    pub fn recv(&mut self, msg: &Self) -> Result<Self, TimestampError> {
        if self.node == msg.node {
            return Err(TimestampError::DuplicatedNode(msg.node));
        }

        let ts = get_unix_timestamp_ms();

        // Unpack the message wall time/counter
        let ts_msg = msg.millis;
        let c_msg = msg.counter;

        // Assert the remote clock drift
        if ts_msg.saturating_sub(ts) > MAX_DRIFT_MS {
            return Err(TimestampError::ClockDrift);
        }

        // Unpack the clock.timestamp logical time and counter
        let ts_old = self.millis;
        let c_old = self.counter;

        // Calculate the next logical time and counter.
        // Ensure that the logical time never goes backward;
        // * if all logical clocks are equal, increment the max counter,
        // * if max = old > message, increment local counter,
        // * if max = message > old, increment message counter,
        // * otherwise, clocks are monotonic, reset counter

        let ts_new = cmp::max(cmp::max(ts_old, ts), ts_msg);

        if ts_new.saturating_sub(ts) > MAX_DRIFT_MS {
            return Err(TimestampError::ClockDrift);
        }

        let c_new = {
            if ts_new == ts_old && ts_new == ts_msg {
                cmp::max(c_old, c_msg)
                    .checked_add(1)
                    .ok_or(TimestampError::Overflow)?
            } else if ts_new == ts_old {
                c_old.checked_add(1).ok_or(TimestampError::Overflow)?
            } else if ts_new == ts_msg {
                c_msg.checked_add(1).ok_or(TimestampError::Overflow)?
            } else {
                0
            }
        };

        self.millis = ts_new;
        self.counter = c_new;

        Ok(Self::new(self.millis, self.counter, msg.node))
    }
}

impl Display for HLCTimestamp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{:0>4X}-{:0>16}",
            self.millis, self.counter, self.node
        )
    }
}

impl FromStr for HLCTimestamp {
    type Err = InvalidFormat;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut splits = s.splitn(2, '-');
        let millis = splits
            .next()
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or(InvalidFormat)?;
        let counter = splits
            .next()
            .and_then(|v| u16::from_str_radix(v, 16).ok())
            .ok_or(InvalidFormat)?;
        let node = splits
            .next()
            .and_then(|v| v.parse::<u32>().ok())
            .ok_or(InvalidFormat)?;

        Ok(Self::new(millis, counter, node))
    }
}

#[derive(Debug, Copy, Clone)]
pub struct InvalidFormat;
impl Display for InvalidFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "InvalidFormat")
    }
}
impl Error for InvalidFormat {}

#[derive(Debug, thiserror::Error)]
pub enum TimestampError {
    #[error("Expected a different unique node, got node with the same id. {0:?}")]
    DuplicatedNode(u32),

    #[error("The clock drift difference is too high to be used.")]
    ClockDrift,

    #[error("The timestamp counter is beyond the capacity of a u16 integer.")]
    Overflow,
}

pub fn get_unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
