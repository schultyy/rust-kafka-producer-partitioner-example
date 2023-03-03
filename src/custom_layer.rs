// Credit: https://github.com/bryanburgers/tracing-blog-post/blob/main/examples/figure_3/custom_layer.rs
use tracing::Level;
use tracing_subscriber::Layer;

pub struct CustomLayer {
    kafka_broker: String,
}

use std::{collections::{BTreeMap}, time::Duration};
use kafka::producer::{Record, Producer, RequiredAcks};
impl CustomLayer {
    pub fn new(kafka_broker: &str) -> Self {
        Self {
            kafka_broker: kafka_broker.into(),
        }
    }

    fn send_event(&self, topic: &str, log_level: &Level, serialized_event: &str) {
        let mut producer = self.create_producer();
        let partition_key = log_level.as_str();

        let record = Record {
            topic: &self.to_kafka_topic_name(topic),
            key: partition_key,
            partition: -1,
            value: serialized_event.as_bytes(),
        };

        producer.send(&record).unwrap();
    }

    fn to_kafka_topic_name(&self, input: &str) -> String {
        input.replace('_', "").replace("::", "-")
    }

    fn create_producer(&self) -> Producer {
        Producer::from_hosts(vec![self.kafka_broker.to_owned()])
            .with_ack_timeout(Duration::from_secs(1))
            .with_required_acks(RequiredAcks::One)
            .create()
            .unwrap()
    }
}

impl<S> Layer<S> for CustomLayer
where
    S: tracing::Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        // Covert the values into a JSON object
        let mut fields = BTreeMap::new();
        let mut visitor = JsonVisitor(&mut fields);
        event.record(&mut visitor);

        // Output the event in JSON
        let output = serde_json::json!({
            "target": event.metadata().target(),
            "name": event.metadata().name(),
            "level": format!("{:?}", event.metadata().level()),
            "fields": fields,
        });
        let serialized_event = serde_json::to_string_pretty(&output).unwrap();
        println!("{}", serialized_event);
        self.send_event(
            event.metadata().target(),
            event.metadata().level(),
            &serialized_event,
        );
    }
}

struct JsonVisitor<'a>(&'a mut BTreeMap<String, serde_json::Value>);

impl<'a> tracing::field::Visit for JsonVisitor<'a> {
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.0
            .insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0
            .insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0
            .insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.0
            .insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0
            .insert(field.name().to_string(), serde_json::json!(value));
    }

    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.0.insert(
            field.name().to_string(),
            serde_json::json!(value.to_string()),
        );
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(
            field.name().to_string(),
            serde_json::json!(format!("{:?}", value)),
        );
    }
}
