
use std::time::{SystemTime, UNIX_EPOCH};


trait TimeProvider {
    fn unix_ts_ms (&self);
}

#[derive(Debug, Clone)]
pub struct SystemTimeProvider {
}

impl SystemTimeProvider {//TimeProvider for
    pub fn unix_ts_ms (&self) -> u64 {
        let dur = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        (dur.as_secs() * 1_000) + dur.subsec_millis() as u64
    }
}

// #[derive(Debug, Clone)]
// pub struct FixedTimeProvider {
//     fixed_unix_ts_ms: u64,
// }

// impl FixedTimeProvider {//TimeProvider for
//     pub fn unix_ts_ms (&self) -> u64 {
//         self.fixed_unix_ts_ms
//     }
// }
