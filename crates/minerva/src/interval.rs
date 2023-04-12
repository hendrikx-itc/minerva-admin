use std::time::Duration;

use regex::Regex;

pub fn parse_interval(interval_str: &str) -> Result<Duration, humantime::DurationError> {
    let interval_re = Regex::new(r"^(\d{2}):(\d{2}):(\d{2})$").unwrap();

    let capture_result = interval_re.captures(interval_str);

    let interval_str: String = match capture_result {
        Some(cap) => {
            format!("{} hours {} minutes {} seconds", &cap[1], &cap[2], &cap[3])
        }
        None => interval_str
            .replace("mon", "month")
            .replace("mons", "month")
            .replace("monthth", "month"),
    };

    humantime::parse_duration(&interval_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_interval() {
        let dur = parse_interval("00:01:00").unwrap();

        let expected_dur = Duration::new(60, 0);

        assert_eq!(dur, expected_dur);
    }
}
