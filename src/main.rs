use tracing::{info, warn};
use tracing_subscriber::prelude::*;

mod custom_layer;
use custom_layer::CustomLayer;

fn main() {
    // Set up how `tracing-subscriber` will deal with tracing data.
    tracing_subscriber::registry().with(CustomLayer::new("broker:9092")).init();

    // Log something simple. In `tracing` parlance, this creates an "event".
    info!(a_bool = true, answer = 42, message = "first example");
    warn!(a_bool = true, answer = 42, message = "first example");
}
