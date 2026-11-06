//! What the crate logs, for its tests: the events of a call, each with its level and message.
use std::sync::{Arc, Mutex};
use tracing::field::{Field, Visit};
use tracing::{Level, span};

// Keeps the level and the message of each event.
struct Recorder(Arc<Mutex<Vec<(Level, String)>>>);

impl tracing::Subscriber for Recorder {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn event(&self, event: &tracing::Event<'_>) {
        struct Message(String);
        impl Visit for Message {
            fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                }
            }
        }
        let mut message = Message(String::new());
        event.record(&mut message);
        let level = *event.metadata().level();
        self.0.lock().unwrap().push((level, message.0));
    }

    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }

    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}

    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}

    fn enter(&self, _: &span::Id) {}

    fn exit(&self, _: &span::Id) {}
}

/// What `call` returns, and the events that it logs on this thread, in their order.
pub(crate) fn logged<T>(call: impl FnOnce() -> T) -> (T, Vec<(Level, String)>) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let value = tracing::subscriber::with_default(Recorder(events.clone()), call);
    let events = std::mem::take(&mut *events.lock().unwrap());
    (value, events)
}

/// The messages of the warnings among `events`.
pub(crate) fn warnings(events: &[(Level, String)]) -> Vec<&str> {
    events
        .iter()
        .filter(|(level, _)| *level == Level::WARN)
        .map(|(_, message)| message.as_str())
        .collect()
}
