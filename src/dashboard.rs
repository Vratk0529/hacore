use crate::db;
use crate::mqtt;
use crate::types::Value;
use std::sync::{Mutex, OnceLock};

static DASHBOARD_HANDLER: OnceLock<Mutex<Option<fn(&str, &Value)>>> = OnceLock::new();
pub fn set_handler(handler: fn(&str, &Value)) {
    DASHBOARD_HANDLER
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap()
        .replace(handler);
}

fn parse_value(payload: &str) -> Value {
    if payload == "true" {
        return Value::Bool(true);
    }
    if payload == "false" {
        return Value::Bool(false);
    }

    if let Ok(v) = payload.parse::<i64>() {
        return Value::Int(v);
    }

    if let Ok(v) = payload.parse::<f64>() {
        return Value::Float(v);
    }

    Value::Text(payload.to_string())
}

fn dashboard_mqtt_handler(topic: String, payload: String, _qos: mqtt::QoS) {
    let key = topic.strip_prefix("dashboard/TX/").unwrap_or(&topic);

    let val = parse_value(&payload);

    db::store_variable(key, val.clone()).ok();

    if let Some(handler) = DASHBOARD_HANDLER
        .get()
        .and_then(|h| h.lock().ok())
        .and_then(|h| *h)
    {
        handler(key, &val);
    }
}

pub fn init() {
    // only listen to TX (incoming from dashboard)
    mqtt::attach_handler(
        "dashboard/TX/#",
        mqtt::QoS::AtLeastOnce,
        dashboard_mqtt_handler,
    );
}

pub fn set(key: &str, val: Value) {
    // publish to RX (outgoing to dashboard)
    let topic = format!("dashboard/RX/{}", key);

    mqtt::publish(
        &topic,
        val.to_string_value().as_str(),
        mqtt::QoS::AtLeastOnce,
    );

    // store internally (without prefix)
    db::store_variable(key, val).ok();
}

pub fn get(key: &str) -> Option<Value> {
    db::read_variable(key).ok()
}
