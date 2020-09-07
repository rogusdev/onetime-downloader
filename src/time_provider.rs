
use std::time::{SystemTime, UNIX_EPOCH};
use dyn_clonable::clonable;


// https://stackoverflow.com/questions/51822118/why-can-a-function-on-a-trait-object-not-be-called-when-bounded-with-self-size
// https://stackoverflow.com/questions/42620022/why-does-a-generic-method-inside-a-trait-require-trait-object-to-be-sized
// https://www.reddit.com/r/rust/comments/7q3bz8/trait_object_with_clone/
// https://stackoverflow.com/questions/26212397/references-to-traits-in-structs
// https://doc.rust-lang.org/book/ch17-02-trait-objects.html
// https://stackoverflow.com/questions/28219519/are-polymorphic-variables-allowed
// https://stackoverflow.com/questions/30353462/how-to-clone-a-struct-storing-a-boxed-trait-object
// https://stackoverflow.com/questions/50017987/cant-clone-vecboxtrait-because-trait-cannot-be-made-into-an-object
#[clonable]
pub trait TimeProvider : Clone {
    fn unix_ts_ms (&self) -> i64;
}

#[derive(Debug, Clone)]
pub struct SystemTimeProvider {
}

impl TimeProvider for SystemTimeProvider {
    fn unix_ts_ms (&self) -> i64 {
        let dur = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        ((dur.as_secs() * 1_000) + dur.subsec_millis() as u64) as i64
    }
}

#[derive(Debug, Clone)]
pub struct FixedTimeProvider {
    fixed_unix_ts_ms: i64,
}

impl FixedTimeProvider {
    #[allow(dead_code)]
    pub fn set_fixed_unix_ts_ms (&mut self, new_unix_ts_ms: i64) {
        self.fixed_unix_ts_ms = new_unix_ts_ms;
    }
}

impl TimeProvider for FixedTimeProvider {
    fn unix_ts_ms (&self) -> i64 {
        self.fixed_unix_ts_ms
    }
}
