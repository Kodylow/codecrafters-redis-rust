use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Returns the current time in milliseconds.
pub fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}

/// Converts a duration in milliseconds to a timestamp in the future as u64.
pub fn millis_to_timestamp_from_now(millis: u64) -> Result<u64, anyhow::Error> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| anyhow::anyhow!("SystemTime before UNIX EPOCH"))?
        .as_millis() as u64;
    Ok(now + millis)
}

/// Converts a timestamp in u64 to a duration from now.
pub fn _timestamp_to_duration_from_now(timestamp: u64) -> Result<Duration, anyhow::Error> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| anyhow::anyhow!("SystemTime before UNIX EPOCH"))?
        .as_millis() as u64;

    if timestamp > now {
        Ok(Duration::from_millis(timestamp - now))
    } else {
        Ok(Duration::from_millis(0))
    }
}

/// Calculates the duration until a timestamp in milliseconds.
pub fn _duration_until_timestamp(timestamp: u64) -> Duration {
    match _timestamp_to_duration_from_now(timestamp) {
        Ok(duration) => duration,
        Err(_) => Duration::from_secs(0), // Default to 0 if there's an error
    }
}
