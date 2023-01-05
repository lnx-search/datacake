use std::cmp;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "rkyv-support")]
use bytecheck::CheckBytes;
#[cfg(feature = "rkyv-support")]
use rkyv::{Archive, Deserialize, Serialize};

/// Maximum physical clock drift allowed, in s
const MAX_DRIFT_S: u64 = 4_100;
pub const COUNTER_MAX: u32 = (1 << 22) - 1;
pub const TIMESTAMP_MAX: u64 = (1 << 34) - 1;

#[derive(Debug, Hash, Copy, Clone, Eq, PartialEq, PartialOrd, Ord)]
#[repr(C)]
#[cfg_attr(feature = "rkyv-support", derive(Serialize, Deserialize, Archive))]
#[cfg_attr(feature = "rkyv-support", archive(compare(PartialEq)))]
#[cfg_attr(
    feature = "rkyv-support",
    archive_attr(repr(C), derive(CheckBytes, Debug))
)]
/// A HLC (Hybrid Logical Clock) timestamp implementation.
///
/// This implementation is largely a port of the JavaScript implementation
/// by @jlongster as provided here: <https://github.com/jlongster/crdt-example-app>
///
/// The demo its self implemented the concepts talked about in this talk:
/// <https://www.youtube.com/watch?v=DEcwa68f-jY>
///
/// The timestamp doubles as a lock which can be used to maintain and consistently
/// unique and monotonic clock.
///
/// This internally is a packed `u64` integer and breaks down as the following:
/// - 34 bits for timestamp (seconds)
/// - 22 bits for counter
/// - 8 bits for node id
///
/// ```
/// use datacake_crdt::{HLCTimestamp, get_unix_timestamp};
///
/// // Let's make two clocks, but we'll refer to them as our nodes, node-a and node-b.
/// let mut node_a = HLCTimestamp::new(get_unix_timestamp(), 0, 0);
///
/// // Node-b has a clock drift of 5 seconds.
/// let mut node_b = HLCTimestamp::new(get_unix_timestamp() + 5, 0, 1);
///
/// // Node-b sends a payload with a new timestamp which we get by calling `send()`.
/// // this makes sure our timestamp is unique and monotonic.
/// let timestamp = node_b.send().unwrap();
///
/// // Node-a gets this payload with the timestamp and so we call `recv()` on our clock.
/// // This was node-a is also unique and monotonic.
/// node_a.recv(&timestamp).unwrap();
/// ```
pub struct HLCTimestamp(u64);

impl HLCTimestamp {
    /// Create a new [HLCTimestamp].
    ///
    /// You may wish to use [get_unix_timestamp] to get a unix timestamp
    /// suitable for the initial clock state.
    pub const fn new(seconds: u64, counter: u32, node: u8) -> Self {
        assert!(
            seconds <= TIMESTAMP_MAX,
            "Timestamp cannot go beyond the maximum capacity of 34 bits. Has 500 years elapsed?",
        );
        assert!(
            counter <= COUNTER_MAX,
            "Counter cannot go beyond the maximum capacity of 22 bits. Has 500 years elapsed?",
        );

        Self(pack(seconds, counter, node))
    }

    #[inline]
    pub fn node(&self) -> u8 {
        (self.0 & 0xFF).try_into().unwrap_or_default()
    }

    #[inline]
    pub fn counter(&self) -> u32 {
        ((self.0 >> 8) & 0b1111111111111111111111) // 22 bits explicitly.
            .try_into()
            .unwrap_or_default()
    }

    #[inline]
    pub fn seconds(&self) -> u64 {
        self.0 >> 30
    }

    /// Timestamp send. Generates a unique, monotonic timestamp suitable
    /// for transmission to another system.
    pub fn send(&mut self) -> Result<Self, TimestampError> {
        let ts = get_unix_timestamp();

        let ts_old = self.seconds();
        let c_old = self.counter();

        // Calculate the next logical time and counter
        // * ensure that the logical time never goes backward
        // * increment the counter if phys time does not advance
        let ts_new = cmp::max(ts_old, ts);

        if ts_new.saturating_sub(ts) > MAX_DRIFT_S {
            return Err(TimestampError::ClockDrift);
        }

        let c_new = if ts_old == ts_new {
            safe_inc_counter(c_old).ok_or(TimestampError::Overflow)?
        } else {
            0
        };

        self.0 = pack(ts_new, c_new, self.node());

        Ok(*self)
    }

    /// Timestamp receive. Parses and merges a timestamp from a remote
    /// system with the local time-global uniqueness and monotonicity are
    /// preserved.
    pub fn recv(&mut self, msg: &Self) -> Result<Self, TimestampError> {
        if self.node() == msg.node() {
            return Err(TimestampError::DuplicatedNode(msg.node()));
        }

        let ts = get_unix_timestamp();

        // Unpack the message wall time/counter
        let ts_msg = msg.seconds();
        let c_msg = msg.counter();

        // Assert the remote clock drift
        if ts_msg.saturating_sub(ts) > MAX_DRIFT_S {
            return Err(TimestampError::ClockDrift);
        }

        // Unpack the clock.timestamp logical time and counter
        let ts_old = self.seconds();
        let c_old = self.counter();

        // Calculate the next logical time and counter.
        // Ensure that the logical time never goes backward;
        // * if all logical clocks are equal, increment the max counter,
        // * if max = old > message, increment local counter,
        // * if max = message > old, increment message counter,
        // * otherwise, clocks are monotonic, reset counter

        let ts_new = cmp::max(cmp::max(ts_old, ts), ts_msg);

        if ts_new.saturating_sub(ts) > MAX_DRIFT_S {
            return Err(TimestampError::ClockDrift);
        }

        let c_new = {
            if ts_new == ts_old && ts_new == ts_msg {
                safe_inc_counter(cmp::max(c_old, c_msg))
                    .ok_or(TimestampError::Overflow)?
            } else if ts_new == ts_old {
                safe_inc_counter(c_old).ok_or(TimestampError::Overflow)?
            } else if ts_new == ts_msg {
                safe_inc_counter(c_msg).ok_or(TimestampError::Overflow)?
            } else {
                0
            }
        };

        self.0 = pack(ts_new, c_new, self.node());

        Ok(Self::new(self.seconds(), self.counter(), msg.node()))
    }
}

impl Display for HLCTimestamp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{:0>6X}-{:0>4}",
            self.seconds(),
            self.counter(),
            self.node()
        )
    }
}

impl FromStr for HLCTimestamp {
    type Err = InvalidFormat;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut splits = s.splitn(3, '-');

        let millis = splits
            .next()
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or(InvalidFormat)?;
        let counter = splits
            .next()
            .and_then(|v| u32::from_str_radix(v, 16).ok())
            .ok_or(InvalidFormat)?;
        let node = splits
            .next()
            .and_then(|v| v.parse::<u8>().ok())
            .ok_or(InvalidFormat)?;

        Ok(Self::new(millis, counter, node))
    }
}

/// Packs the given values into
const fn pack(seconds: u64, counter: u32, node: u8) -> u64 {
    let counter = counter as u64;
    let node = node as u64;

    (seconds << 30) | (counter << 8) | node
}

/// Increments the counter value respecting the maximum
/// allowed value which is safe to pack.
const fn safe_inc_counter(v: u32) -> Option<u32> {
    if (v + 1) > COUNTER_MAX {
        None
    } else {
        Some(v + 1)
    }
}

#[derive(Debug, Copy, Clone, thiserror::Error)]
#[error("Invalid timestamp format.")]
/// The provided timestamp in the given string format is invalid and unable to be parsed.
pub struct InvalidFormat;

#[derive(Debug, thiserror::Error)]
pub enum TimestampError {
    #[error("Expected a different unique node, got node with the same id. {0:?}")]
    DuplicatedNode(u8),

    #[error("The clock drift difference is too high to be used.")]
    ClockDrift,

    #[error("The timestamp counter is beyond the capacity of a u16 integer.")]
    Overflow,
}

/// Get the current time since the [UNIX_EPOCH] in milliseconds.
pub fn get_unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Get the current time since the [DATACAKE_EPOCH] in seconds.
pub fn get_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_TS: u64 = 1;

    #[test]
    fn test_parse() {
        let ts = HLCTimestamp::new(TEST_TS, 0, 0);

        let str_ts = ts.to_string();
        HLCTimestamp::from_str(&str_ts).expect("Parse timestamp");
    }

    #[test]
    fn test_same_node_error() {
        let mut ts1 = HLCTimestamp::new(TEST_TS, 0, 0);
        let ts2 = HLCTimestamp::new(TEST_TS, 1, 0);

        assert!(matches!(
            ts1.recv(&ts2),
            Err(TimestampError::DuplicatedNode(0)),
        ))
    }

    #[test]
    fn test_clock_drift_error() {
        let mut ts1 = HLCTimestamp::new(get_unix_timestamp(), 0, 0);
        let ts2 = HLCTimestamp::new(ts1.seconds() + MAX_DRIFT_S + 1000, 0, 1);

        assert!(matches!(ts1.recv(&ts2), Err(TimestampError::ClockDrift)));

        let mut ts = HLCTimestamp::new(get_unix_timestamp() + MAX_DRIFT_S + 1000, 0, 1);
        assert!(matches!(ts.send(), Err(TimestampError::ClockDrift)));
    }

    #[test]
    fn test_clock_overflow_error() {
        let mut ts1 = HLCTimestamp::new(get_unix_timestamp(), COUNTER_MAX, 0);
        let ts2 = HLCTimestamp::new(ts1.seconds(), COUNTER_MAX, 1);

        assert!(matches!(ts1.recv(&ts2), Err(TimestampError::Overflow)));
    }

    #[test]
    fn test_timestamp_send() {
        let mut ts1 = HLCTimestamp::new(get_unix_timestamp(), 0, 0);
        let ts2 = ts1.send().unwrap();
        assert_eq!(ts1.seconds(), ts2.seconds(), "Logical clock should match.");
        assert_eq!(ts1.counter(), 1, "Counter should be incremented for ts1.");
        assert_eq!(ts2.counter(), 1, "Counter should be incremented for ts2.");
    }

    #[test]
    fn test_timestamp_recv() {
        let mut ts1 = HLCTimestamp::new(get_unix_timestamp(), 0, 0);
        let mut ts2 = HLCTimestamp::new(ts1.seconds(), 3, 1);

        let ts3 = ts1.recv(&ts2).unwrap();

        // Ts3 is just a copy of the clock itself at this point.
        assert_eq!(ts1.seconds(), ts3.seconds());
        assert_eq!(ts1.counter(), ts3.counter());

        assert_eq!(ts3.counter(), 4); // seconds stay the same, our counter should increment.

        let ts4 = ts2.recv(&ts1).unwrap();
        assert_eq!(ts2.seconds(), ts4.seconds());
        assert_eq!(ts2.counter(), ts4.counter());
        assert_eq!(ts4.counter(), 5); // seconds stay the same, our counter should increment.

        assert!(ts1 < ts2);
        assert!(ts3 < ts4);
    }

    #[test]
    fn test_timestamp_ordering() {
        let ts1 = HLCTimestamp::new(TEST_TS, 0, 0);
        let ts2 = HLCTimestamp::new(TEST_TS, 1, 0);
        let ts3 = HLCTimestamp::new(TEST_TS, 2, 0);
        assert!(ts1 < ts2);
        assert!(ts2 < ts3);

        let ts1 = HLCTimestamp::new(TEST_TS, 0, 0);
        let ts2 = HLCTimestamp::new(TEST_TS, 0, 1);
        assert!(ts1 < ts2);

        let ts1 = HLCTimestamp::new(TEST_TS, 0, 1);
        let ts2 = HLCTimestamp::new(TEST_TS + 1, 0, 0);
        assert!(ts1 < ts2);

        let mut ts1 = HLCTimestamp::new(get_unix_timestamp(), 0, 1);
        let ts2 = ts1.send().unwrap();
        let ts3 = ts1.send().unwrap();
        assert!(ts2 < ts3);

        let mut ts1 = HLCTimestamp::new(get_unix_timestamp(), 0, 0);
        let ts2 = HLCTimestamp::new(ts1.seconds(), 1, 1);
        let _ts3 = ts1.recv(&ts2).unwrap();
        assert!(ts1 > ts2);
    }
}

#[cfg(all(test, feature = "rkyv-support"))]
mod rkyv_tests {
    use super::*;

    #[test]
    fn test_serialize() {
        let ts = HLCTimestamp::new(get_unix_timestamp(), 0, 0);
        rkyv::to_bytes::<_, 1024>(&ts).expect("Serialize timestamp OK");
    }

    #[test]
    fn test_deserialize() {
        let ts = HLCTimestamp::new(get_unix_timestamp(), 0, 0);
        let buffer = rkyv::to_bytes::<_, 1024>(&ts).expect("Serialize timestamp OK");

        let new_ts: HLCTimestamp =
            rkyv::from_bytes(&buffer).expect("Deserialize timestamp OK");
        assert_eq!(ts, new_ts);
    }
}
