use mqtt::AsyncClient;
use paho_mqtt as mqtt;
pub use paho_mqtt::DeliveryToken;
pub use paho_mqtt::QoS;
use std::sync::Arc;
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::Duration;

struct VarAttachment {
    topic: String,
    qos: QoS,
    target: Arc<Mutex<String>>,
}
struct FnAttachment {
    topic: String,
    qos: QoS,
    handler: fn(String, String, QoS),
}

static VAR_ATTACHMENTS: OnceLock<Mutex<Vec<VarAttachment>>> = OnceLock::new();
static FN_ATTACHMENTS: OnceLock<Mutex<Vec<FnAttachment>>> = OnceLock::new();

fn var_attachments() -> MutexGuard<'static, Vec<VarAttachment>> {
    VAR_ATTACHMENTS
        .get()
        .expect("var_attachments not initialized")
        .lock()
        .unwrap()
}
fn fn_attachments() -> MutexGuard<'static, Vec<FnAttachment>> {
    FN_ATTACHMENTS
        .get()
        .expect("fn_attachments not initialized")
        .lock()
        .unwrap()
}
struct MessageData {
    topic: String,
    payload: String,
}

static MESSAGE_BUFFER: OnceLock<Mutex<Vec<MessageData>>> = OnceLock::new();
fn message_buffer() -> MutexGuard<'static, Vec<MessageData>> {
    MESSAGE_BUFFER
        .get()
        .expect("buffer not initialized")
        .lock()
        .unwrap()
}

static CLIENT: OnceLock<Mutex<AsyncClient>> = OnceLock::new();
pub fn client() -> MutexGuard<'static, AsyncClient> {
    CLIENT
        .get()
        .expect("client not initialized")
        .lock()
        .unwrap()
}

fn topic_matches(filter: &str, topic: &str) -> bool {
    let mut f_iter = filter.split('/');
    let mut t_iter = topic.split('/');

    loop {
        match (f_iter.next(), t_iter.next()) {
            (Some("#"), _) => return true,

            (Some("+"), Some(_)) => continue,

            (Some(f), Some(t)) if f == t => continue,

            (None, None) => return true,

            _ => return false,
        }
    }
}

fn message_callback(_client: &AsyncClient, msg: Option<mqtt::Message>) {
    if let Some(msg) = msg {
        let handler = {
            let attachments = fn_attachments();

            attachments
                .iter()
                .find(|a| topic_matches(&a.topic, msg.topic()))
                .map(|a| a.handler)
        };

        if let Some(handler) = handler {
            let topic = msg.topic().to_string();
            let payload = msg.payload_str().to_string();
            let qos = msg.qos();

            std::thread::spawn(move || {
                handler(topic, payload, qos);
            });

            return;
        }

        let data = MessageData {
            topic: msg.topic().to_string(),
            payload: msg.payload_str().to_string(),
        };

        message_buffer().push(data);
    }
}

/// Called by paho on every successful connection -- the initial one *and*
/// every automatic reconnect. Paho's automatic reconnect only re-opens the
/// TCP/MQTT connection; it does not remember subscriptions on its own, so
/// we resubscribe everything that was ever registered via `attach_handler`
/// / `attach_variable` / `create_var`.
///
/// Never block in here (no `.wait()`): this runs on the client's internal
/// thread, and blocking it can deadlock the client.
fn on_connected(cli: &AsyncClient) {
    let handler_topics = fn_attachments().len();
    let var_topics = var_attachments().len();
    println!(
        "hacore::mqtt: connected, restoring {} subscription(s)",
        handler_topics + var_topics
    );
    for att in fn_attachments().iter() {
        let _ = cli.subscribe(&att.topic, att.qos);
    }
    for att in var_attachments().iter() {
        let _ = cli.subscribe(&att.topic, att.qos);
    }
}

fn on_connection_lost(_cli: &AsyncClient) {
    eprintln!("hacore::mqtt: connection lost, reconnecting...");
}

pub fn init() {
    let _client = mqtt::AsyncClient::new(
        mqtt::create_options::CreateOptionsBuilder::new()
            .server_uri(std::env::var("MQTT_SERVER_URI").unwrap())
            .client_id(std::env::var("MQTT_CLIENT_ID").unwrap())
            .finalize(),
    )
    .unwrap();

    // These need to exist *before* we connect: on_connected fires for the
    // initial connection too, and reads them.
    if VAR_ATTACHMENTS
        .set(Mutex::new(<Vec<VarAttachment>>::new()))
        .is_err()
    {
        panic!("var_ttachments already initialised");
    }
    if FN_ATTACHMENTS
        .set(Mutex::new(<Vec<FnAttachment>>::new()))
        .is_err()
    {
        panic!("fn_attachments already initialised");
    }
    if MESSAGE_BUFFER
        .set(Mutex::new(<Vec<MessageData>>::new()))
        .is_err()
    {
        panic!("Message buffer already initialised");
    }

    _client.set_message_callback(message_callback);
    _client.set_connected_callback(on_connected);
    _client.set_connection_lost_callback(on_connection_lost);

    // How aggressively paho retries a dropped connection: starts at
    // MQTT_RECONNECT_MIN_SECS after a drop, backs off up to
    // MQTT_RECONNECT_MAX_SECS between attempts. Both are optional; sane
    // defaults are used if unset.
    let min_retry = Duration::from_secs(
        std::env::var("MQTT_RECONNECT_MIN_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1),
    );
    let max_retry = Duration::from_secs(
        std::env::var("MQTT_RECONNECT_MAX_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30),
    );

    _client
        .connect(
            mqtt::ConnectOptionsBuilder::new()
                .user_name(std::env::var("MQTT_USERNAME").unwrap())
                .password(std::env::var("MQTT_PASSWORD").unwrap())
                .automatic_reconnect(min_retry, max_retry)
                .finalize(),
        )
        .wait()
        .unwrap();

    if CLIENT.set(Mutex::new(_client)).is_err() {
        panic!("Client already initialised");
    }
}

pub fn fetch_values() {
    let mut buffer = message_buffer();
    let var_attachments = var_attachments();

    for msg in buffer.iter() {
        for att in var_attachments.iter() {
            if att.topic == msg.topic {
                let mut target = att.target.lock().unwrap();
                *target = msg.payload.clone();
            }
        }
    }

    buffer.clear();
}

pub fn attach_handler(topic: &str, qos: QoS, handler: fn(String, String, QoS)) {
    fn_attachments().push(FnAttachment {
        topic: topic.to_string(),
        qos,
        handler,
    });
    client().subscribe(topic, qos).wait().unwrap();
}

pub fn create_var(topic: &str, qos: mqtt::QoS) -> Arc<Mutex<String>> {
    let var = Arc::new(Mutex::new(String::new()));
    attach_variable(topic, qos, var.clone());
    return var;
}

pub fn attach_variable(topic: &str, qos: QoS, var: Arc<Mutex<String>>) {
    var_attachments().push(VarAttachment {
        topic: topic.to_string(),
        qos,
        target: var,
    });
    client().subscribe(topic, qos).wait().unwrap();
}

pub fn publish(topic: &str, payload: &str, qos: QoS) -> DeliveryToken {
    return client().publish(paho_mqtt::Message::new(topic, payload, qos));
}
