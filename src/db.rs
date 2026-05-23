use crate::types;
use sqlite::Connection;
use std::sync::{Mutex, MutexGuard, OnceLock};

use std::collections::HashMap;

static CACHE: OnceLock<Mutex<HashMap<String, types::Value>>> = OnceLock::new();
fn cache() -> std::sync::MutexGuard<'static, HashMap<String, types::Value>> {
    CACHE.get().expect("cache not initialized").lock().unwrap()
}

static DATABASE: OnceLock<Mutex<Connection>> = OnceLock::new();
fn db() -> MutexGuard<'static, Connection> {
    DATABASE
        .get()
        .expect("database not initialized")
        .lock()
        .unwrap()
}

pub fn init() -> Result<(), types::DbError> {
    let conn = sqlite::open("db.sqlite")?;
    conn.execute(
        "
    CREATE TABLE variables (key TEXT PRIMARY KEY, type TEXT, value);
    ",
    )
    .ok();

    conn.execute("PRAGMA journal_mode=WAL;").ok();
    conn.execute("PRAGMA synchronous=NORMAL;").ok();

    if DATABASE.set(Mutex::new(conn)).is_err() {
        return Err(types::DbError::DbInitialised);
    }

    CACHE.set(Mutex::new(HashMap::new())).unwrap();

    return Ok(());
}
pub fn store_variable(key: &str, val: types::Value) -> Result<(), types::DbError> {
    {
        let mut cache = cache();
        cache.insert(key.to_string(), val.clone());
    }

    let db = db();
    match val {
        types::Value::Text(v) => {
            db.execute(format!(
                "INSERT OR REPLACE INTO variables (key,value,type) VALUES ('{}','{}','string')",
                key, v
            ))?;
        }
        types::Value::Int(v) => {
            db.execute(format!(
                "INSERT OR REPLACE INTO variables (key,value,type) VALUES ('{}','{}','int')",
                key, v
            ))?;
        }
        types::Value::Float(v) => {
            db.execute(format!(
                "INSERT OR REPLACE INTO variables (key,value,type) VALUES ('{}','{}','float')",
                key, v
            ))?;
        }
        types::Value::Bool(v) => {
            db.execute(format!(
                "INSERT OR REPLACE INTO variables (key,value,type) VALUES ('{}','{}','bool')",
                key,
                if v { 1 } else { 0 }
            ))?;
        }
        types::Value::None => {}
    }
    Ok(())
}
pub fn read_variable(key: &str) -> Result<types::Value, types::DbError> {
    {
        let cache = cache(); // cache first
        if let Some(val) = cache.get(key) {
            return Ok(val.clone());
        }
    }

    let db = db();

    let mut result: Option<types::Value> = None;
    let mut invalid_type = false;

    db.iterate(
        format!(
            "SELECT value,type FROM variables WHERE key='{}' LIMIT 1",
            key
        ),
        |row| {
            let mut tp = "";
            let mut value = "";

            for pair in row {
                if pair.0 == "value" {
                    if let Some(v) = pair.1 {
                        value = v;
                    }
                }
                if pair.0 == "type" {
                    if let Some(v) = pair.1 {
                        tp = v;
                    }
                }
            }

            result = match tp {
                "string" => Some(types::Value::Text(value.to_string())),
                "int" => value.parse().ok().map(types::Value::Int),
                "float" => value.parse().ok().map(types::Value::Float),
                "bool" => match value {
                    "true" => Some(types::Value::Bool(true)),
                    "false" => Some(types::Value::Bool(false)),
                    "1" => Some(types::Value::Bool(true)),
                    "0" => Some(types::Value::Bool(false)),
                    _ => None,
                },
                _ => {
                    invalid_type = true;
                    None
                }
            };

            true
        },
    )?;

    if invalid_type {
        return Err(types::DbError::InvalidType);
    }

    let value = result.ok_or(types::DbError::NotFound)?;

    {
        // store in cache
        let mut cache = cache();
        cache.insert(key.to_string(), value.clone());
    }

    Ok(value)
}

pub fn read_variable_default(key: &str, default: types::Value) -> types::Value {
    match read_variable(key) {
        Ok(v) => v,
        Err(e) => {
            println!("key: {key}, error: {:?}", e);
            default
        }
    }
}

pub fn get_i64(key: &str, default: i64) -> i64 {
    read_variable_default(key, types::Value::Int(default))
        .as_i64()
        .unwrap()
}

pub fn get_f64(key: &str, default: f64) -> f64 {
    read_variable_default(key, types::Value::Float(default))
        .as_f64()
        .unwrap()
}

pub fn get_bool(key: &str, default: bool) -> bool {
    read_variable_default(key, types::Value::Bool(default))
        .as_bool()
        .unwrap()
}

pub fn get_string(key: &str, default: &str) -> String {
    read_variable_default(key, types::Value::Text(default.to_string()))
        .as_str()
        .unwrap()
        .to_string()
}

pub fn set_i64(key: &str, val: i64) {
    store_variable(key, types::Value::Int(val)).unwrap();
}

pub fn set_f64(key: &str, val: f64) {
    store_variable(key, types::Value::Float(val)).unwrap();
}

pub fn set_bool(key: &str, val: bool) {
    store_variable(key, types::Value::Bool(val)).unwrap();
}

pub fn set_string(key: &str, val: &str) {
    store_variable(key, types::Value::Text(val.to_string())).unwrap();
}
