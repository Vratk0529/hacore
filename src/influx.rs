//! A minimal InfluxDB (1.x line-protocol) writer.
//!
//! Configured the same way as the rest of hacore: via environment variables
//! (loaded by `dotenv` in `init_all`).
//!
//!   INFLUX_URL      default "http://127.0.0.1:8086"
//!   INFLUX_DB       default "home"
//!   INFLUX_USERNAME optional
//!   INFLUX_PASSWORD optional
//!
//! Writes are fire-and-forget: a dead/unreachable InfluxDB logs a warning
//! and is otherwise ignored, so it can never take a controller down.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

struct Config {
    url: String,
    database: String,
    username: Option<String>,
    password: Option<String>,
}

static CONFIG: OnceLock<Config> = OnceLock::new();
static RATE_LIMIT: OnceLock<Mutex<HashMap<String, Instant>>> = OnceLock::new();

pub fn init() {
    let url = std::env::var("INFLUX_URL").unwrap_or_else(|_| "http://127.0.0.1:8086".to_string());
    let database = std::env::var("INFLUX_DB").unwrap_or_else(|_| "home".to_string());
    let username = std::env::var("INFLUX_USERNAME").ok();
    let password = std::env::var("INFLUX_PASSWORD").ok();

    // init_all() may run more than once in tests etc.; don't panic on that.
    let _ = CONFIG.set(Config {
        url,
        database,
        username,
        password,
    });
    let _ = RATE_LIMIT.set(Mutex::new(HashMap::new()));
}

fn config() -> &'static Config {
    CONFIG.get().expect("influx::init() was not called (did you call hacore::init_all()?)")
}

// ---------------------------------------------------------------------------
// Field / Point: a small line-protocol builder
// ---------------------------------------------------------------------------

pub enum Field {
    Float(f64),
    Int(i64),
    Bool(bool),
    Text(String),
}

impl From<f64> for Field {
    fn from(v: f64) -> Self {
        Field::Float(v)
    }
}
impl From<i64> for Field {
    fn from(v: i64) -> Self {
        Field::Int(v)
    }
}
impl From<bool> for Field {
    fn from(v: bool) -> Self {
        Field::Bool(v)
    }
}
impl From<&str> for Field {
    fn from(v: &str) -> Self {
        Field::Text(v.to_string())
    }
}
impl From<String> for Field {
    fn from(v: String) -> Self {
        Field::Text(v)
    }
}

impl Field {
    fn to_line_value(&self) -> String {
        match self {
            Field::Float(v) => v.to_string(),
            Field::Int(v) => format!("{}i", v),
            Field::Bool(v) => v.to_string(),
            Field::Text(v) => format!("\"{}\"", v.replace('\\', "\\\\").replace('"', "\\\"")),
        }
    }
}

/// A single InfluxDB point: a measurement, optional tags, and one or more
/// fields. Build with `Point::new(...).tag(...).field(...)` and hand it to
/// `write`.
pub struct Point {
    measurement: String,
    tags: Vec<(String, String)>,
    fields: Vec<(String, Field)>,
}

impl Point {
    pub fn new(measurement: &str) -> Self {
        Self {
            measurement: measurement.to_string(),
            tags: Vec::new(),
            fields: Vec::new(),
        }
    }

    pub fn tag(mut self, key: &str, value: &str) -> Self {
        self.tags.push((key.to_string(), value.to_string()));
        self
    }

    pub fn field(mut self, key: &str, value: impl Into<Field>) -> Self {
        self.fields.push((key.to_string(), value.into()));
        self
    }

    fn to_line(&self) -> String {
        fn escape_key_or_tag(s: &str) -> String {
            s.replace('\\', "\\\\")
                .replace(',', "\\,")
                .replace(' ', "\\ ")
                .replace('=', "\\=")
        }
        fn escape_measurement(s: &str) -> String {
            s.replace('\\', "\\\\")
                .replace(',', "\\,")
                .replace(' ', "\\ ")
        }

        let mut line = escape_measurement(&self.measurement);
        for (k, v) in &self.tags {
            line.push(',');
            line.push_str(&escape_key_or_tag(k));
            line.push('=');
            line.push_str(&escape_key_or_tag(v));
        }
        line.push(' ');
        let fields: Vec<String> = self
            .fields
            .iter()
            .map(|(k, v)| format!("{}={}", escape_key_or_tag(k), v.to_line_value()))
            .collect();
        line.push_str(&fields.join(","));
        line
    }
}

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

/// Write a point immediately (no rate limiting).
pub fn write(point: Point) {
    let cfg = config();
    let endpoint = format!("{}/write?db={}", cfg.url, cfg.database);
    let line = point.to_line();

    let req = ureq::post(&endpoint);
    let req = match (&cfg.username, &cfg.password) {
        (Some(u), Some(p)) => req.set("Authorization", &format!("Basic {}", base64_encode(&format!("{}:{}", u, p)))),
        _ => req,
    };

    if let Err(e) = req.send_string(&line) {
        eprintln!("hacore::influx write failed: {}", e);
    }
}

/// Convenience for the common case of one measurement with a single
/// `value` field, e.g. `influx::write_value("batt-v", 13.2)`.
pub fn write_value(measurement: &str, value: f64) {
    write(Point::new(measurement).field("value", value));
}

fn rate_limit_ok(key: &str, min_interval: Duration) -> bool {
    let mut map = RATE_LIMIT
        .get()
        .expect("influx::init() was not called")
        .lock()
        .unwrap();
    match map.get(key) {
        Some(t) if t.elapsed() < min_interval => false,
        _ => {
            map.insert(key.to_string(), Instant::now());
            true
        }
    }
}

/// Same as `write`, but drops the point if this measurement was already
/// written within `min_interval`. Handy for high-frequency sensor loops.
pub fn write_throttled(point: Point, min_interval: Duration) {
    if rate_limit_ok(&point.measurement, min_interval) {
        write(point);
    }
}

/// Same as `write_value`, but rate-limited per measurement.
pub fn write_value_throttled(measurement: &str, value: f64, min_interval: Duration) {
    if rate_limit_ok(measurement, min_interval) {
        write_value(measurement, value);
    }
}

// Minimal base64 encoder so basic-auth doesn't need an extra dependency.
fn base64_encode(input: &str) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = input.as_bytes();
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        out.push(CHARS[(b0 >> 2) as usize] as char);
        out.push(CHARS[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        out.push(if chunk.len() > 1 {
            CHARS[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            CHARS[(b2 & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}
