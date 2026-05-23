pub mod db;
pub mod mqtt;
pub mod types;
pub mod dashboard;

pub fn init_all() {
    use dotenv;
    dotenv::dotenv().unwrap();
    db::init().unwrap();
    mqtt::init();
    dashboard::init();
    println!("Connected: {}", mqtt::client().is_connected());
}
