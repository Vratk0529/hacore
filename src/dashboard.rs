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

fn publish_and_store(key: &str, val: Value, retain: bool) {
    // publish to RX (outgoing to dashboard)
    let topic = format!("dashboard/RX/{}", key);

    mqtt::publish_opts(
        &topic,
        val.to_string_value().as_str(),
        mqtt::QoS::AtLeastOnce,
        retain,
    );

    // store internally (without prefix)
    db::store_variable(key, val).ok();
}

pub fn set(key: &str, val: Value) {
    publish_and_store(key, val, false);
}

/// Same as [`set`], but asks the broker to retain the value.
///
/// Home Assistant's MQTT entities have no state until their `state_topic`
/// produces one, so with unretained publishes every HA restart leaves the
/// dashboard showing "unknown" until this controller happens to publish
/// again. Retaining the state topic fixes that -- HA gets the last value
/// the moment it subscribes.
///
/// Only worth using for values that are genuinely state. Pair it with
/// change-detection on the caller's side: a retained publish costs the
/// broker a disk write, so republishing an unchanged value at telemetry
/// rate is a good way to wear out an SD card.
pub fn set_retained(key: &str, val: Value) {
    publish_and_store(key, val, true);
}

pub fn get(key: &str) -> Option<Value> {
    db::read_variable(key).ok()
}
