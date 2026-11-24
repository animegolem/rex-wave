//! The workspace's one civil-calendar and RFC 3339 implementation, on
//! [`UnixMs`]. Day arithmetic is Howard Hinnant's `days_from_civil` /
//! `civil_from_days` for the proleptic Gregorian calendar.

use crate::UnixMs;

/// The latest year these conversions accept.
const MAX_YEAR: i64 = 9_999;

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

/// Days since 1970-01-01 of a valid `year-month-day`.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let m = u64::from(month);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + u64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe as i64 - 719_468
}

/// `(year, month, day)` of a count of days since 1970-01-01.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if month <= 2 { y + 1 } else { y }, month, day)
}

impl UnixMs {
    /// `YYYY-MM-DDTHH:MM:SSZ`: UTC, whole seconds (milliseconds dropped).
    pub fn to_rfc3339_sec(self) -> String {
        let secs = self.0 / 1_000;
        let (year, month, day) = civil_from_days((secs / 86_400) as i64);
        let rem = secs % 86_400;
        format!(
            "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
            rem / 3_600,
            (rem % 3_600) / 60,
            rem % 60
        )
    }

    /// A UTC civil time. `None` when a field is out of range (the day for its
    /// month included), or the instant is before 1970 or after year 9999.
    /// A leap second (`second == 60`) folds into the next minute.
    pub fn from_ymd_hms_utc(
        year: i64,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
    ) -> Option<UnixMs> {
        if !(1_970..=MAX_YEAR).contains(&year)
            || !(1..=12).contains(&month)
            || day == 0
            || day > days_in_month(year, month)
            || hour > 23
            || minute > 59
            || second > 60
        {
            return None;
        }
        let secs = days_from_civil(year, month, day) * 86_400
            + i64::from(hour) * 3_600
            + i64::from(minute) * 60
            + i64::from(second);
        u64::try_from(secs).ok().map(|s| UnixMs(s * 1_000))
    }

    /// `YYYY-MM-DD(T|t| )HH:MM:SS(.fraction)?(Z|z|±HH:MM)?`; a missing zone
    /// is UTC and fraction digits past milliseconds are dropped.
    pub fn parse_rfc3339(raw: &str) -> Option<UnixMs> {
        let s = raw.trim();
        let b = s.as_bytes();
        if b.len() < 19
            || b[4] != b'-'
            || b[7] != b'-'
            || !matches!(b[10], b'T' | b't' | b' ')
            || b[13] != b':'
            || b[16] != b':'
        {
            return None;
        }
        let digits = |range: std::ops::Range<usize>| -> Option<u32> {
            let field = &s[range];
            if field.bytes().all(|c| c.is_ascii_digit()) {
                field.parse().ok()
            } else {
                None
            }
        };
        let base = UnixMs::from_ymd_hms_utc(
            i64::from(digits(0..4)?),
            digits(5..7)?,
            digits(8..10)?,
            digits(11..13)?,
            digits(14..16)?,
            digits(17..19)?,
        )?;

        let mut rest = &s[19..];
        let mut millis = 0u64;
        if let Some(after) = rest.strip_prefix('.') {
            let end = after
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(after.len());
            if end == 0 {
                return None;
            }
            for (i, c) in after[..end].bytes().take(3).enumerate() {
                millis += u64::from(c - b'0') * [100, 10, 1][i];
            }
            rest = &after[end..];
        }

        let offset_ms: i64 = match rest {
            "" | "Z" | "z" => 0,
            tz if tz.len() == 6
                && matches!(tz.as_bytes()[0], b'+' | b'-')
                && tz.as_bytes()[3] == b':' =>
            {
                let sign = if tz.starts_with('-') { -1 } else { 1 };
                let h: i64 = tz[1..3].parse().ok()?;
                let m: i64 = tz[4..6].parse().ok()?;
                if h > 23 || m > 59 {
                    return None;
                }
                sign * (h * 3_600 + m * 60) * 1_000
            }
            _ => return None,
        };
        let ms = i64::try_from(base.0 + millis).ok()? - offset_ms;
        u64::try_from(ms).ok().map(UnixMs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_formats_and_round_trips_at_second_precision() {
        assert_eq!(UnixMs(0).to_rfc3339_sec(), "1970-01-01T00:00:00Z");
        assert_eq!(
            UnixMs(1_795_000_000_999).to_rfc3339_sec(),
            "2026-11-18T11:06:40Z"
        );
        for ms in [0, 951_782_400_000, 1_709_164_800_000, 4_102_444_799_000] {
            let text = UnixMs(ms).to_rfc3339_sec();
            assert_eq!(UnixMs::parse_rfc3339(&text), Some(UnixMs(ms)), "{text}");
        }
    }

    #[test]
    fn civil_dates_validate_month_lengths_and_leap_years() {
        assert!(UnixMs::from_ymd_hms_utc(2024, 2, 29, 0, 0, 0).is_some());
        assert!(UnixMs::from_ymd_hms_utc(2023, 2, 29, 0, 0, 0).is_none());
        assert!(UnixMs::from_ymd_hms_utc(2000, 2, 29, 0, 0, 0).is_some());
        assert!(UnixMs::from_ymd_hms_utc(1900, 2, 29, 0, 0, 0).is_none());
        assert!(UnixMs::from_ymd_hms_utc(2026, 4, 31, 0, 0, 0).is_none());
        assert!(UnixMs::from_ymd_hms_utc(1969, 12, 31, 23, 59, 59).is_none());
        assert_eq!(
            UnixMs::from_ymd_hms_utc(2026, 12, 31, 23, 59, 60),
            UnixMs::from_ymd_hms_utc(2027, 1, 1, 0, 0, 0)
        );
    }

    #[test]
    fn rfc3339_parses_fractions_offsets_and_refuses_junk() {
        let base = UnixMs::from_ymd_hms_utc(2026, 11, 21, 0, 0, 0).unwrap();
        assert_eq!(
            UnixMs::parse_rfc3339("2026-11-21T00:00:00.500Z"),
            Some(UnixMs(base.0 + 500))
        );
        assert_eq!(
            UnixMs::parse_rfc3339("2026-11-21t00:00:00.123456z"),
            Some(UnixMs(base.0 + 123))
        );
        assert_eq!(
            UnixMs::parse_rfc3339("2026-11-21 02:30:00+02:30"),
            Some(base)
        );
        assert_eq!(
            UnixMs::parse_rfc3339("2026-11-20T19:00:00-05:00"),
            Some(base)
        );
        assert_eq!(UnixMs::parse_rfc3339("2026-11-21T00:00:00"), Some(base));
        for bad in [
            "",
            "2026-11-21",
            "2026-13-01T00:00:00Z",
            "2026-02-30T00:00:00Z",
            "2026-11-21T24:00:00Z",
            "2026-11-21T00:00:00.Z",
            "2026-11-21T00:00:00+0200",
            "2026-11-21T00:00:00Q",
            "+026-11-21T00:00:00Z",
        ] {
            assert_eq!(UnixMs::parse_rfc3339(bad), None, "{bad:?}");
        }
    }
}
