pub mod db;
pub mod dashboard;
pub mod influx;
pub mod mqtt;
pub mod types;

pub fn init_all() {
    dotenv::dotenv().ok();
    db::init().unwrap();
    mqtt::init();
    dashboard::init();
    influx::init();
    println!("Connected: {}", mqtt::client().is_connected());
}