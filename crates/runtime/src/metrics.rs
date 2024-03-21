use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
    SharedString, Unit,
};
use std::fmt::{self, Display, Formatter};
use std::sync::RwLock;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc;

// A [`metrics`][metrics]-compatible exporter that keeps track of gauge metrics in memory. It
// explicitly discards both ['Counter'][Counters] and [`Histogram`][Histograms]. This is useful
// to track and query the status of components within the runtime (e.g. spicepods, datasets, etc).
// [`LocalGaugeRecorder`] supports tracking metrics given a fixed or regex prefix.

#[derive(Clone)]
pub struct LocalGaugeRecorder {
    operations_sender: mpsc::Sender<GaugeOperation>,

    #[allow(dead_code)]
    gauges: Arc<RwLock<HashMap<Key, f64>>>,
}
impl Default for LocalGaugeRecorder {
    fn default() -> Self {
        Self::new()
    }
}

enum GaugeOperation {
    Increment(Key, f64),
    Decrement(Key, f64),
    Set(Key, f64),
}

impl Display for GaugeOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            GaugeOperation::Increment(key, value) => {
                write!(f, "Increment({key}, {value})")
            }
            GaugeOperation::Decrement(key, value) => {
                write!(f, "Decrement({key}, {value})")
            }
            GaugeOperation::Set(key, value) => {
                write!(f, "Set({key}, {value})")
            }
        }
    }
}

impl LocalGaugeRecorder {
    #[must_use]
    pub fn new() -> Self {
        let (operations_sender, mut operations_receiver) = mpsc::channel(100);
        let gauges: Arc<RwLock<HashMap<Key, f64>>> = Arc::new(RwLock::new(HashMap::new()));

        let recv_gauges: Arc<RwLock<HashMap<Key, f64>>> = gauges.clone();
        tokio::spawn(async move {
            while let Some(operation) = operations_receiver.recv().await {
                println!("Received operation: {operation}. Gauges: {recv_gauges:#?}");
                match recv_gauges.write() {
                    Ok(mut writable_gauges) => match operation {
                        GaugeOperation::Increment(key, value) => {
                            if let Some(gauge) = writable_gauges.get_mut(&key) {
                                *gauge += value;
                            } else {
                                writable_gauges.insert(key, value);
                            }
                        }
                        GaugeOperation::Decrement(key, value) => {
                            if let Some(gauge) = writable_gauges.get_mut(&key) {
                                *gauge -= value;
                            } else {
                                writable_gauges.insert(key, -value);
                            }
                        }
                        GaugeOperation::Set(key, value) => {
                            if let Some(gauge) = writable_gauges.get_mut(&key) {
                                *gauge = value;
                            } else {
                                writable_gauges.insert(key, value);
                            }
                        }
                    },
                    Err(e) => {
                        println!("Error writing gauges: {e:#?}");
                    }
                };
            }
        });
        LocalGaugeRecorder {
            operations_sender,
            gauges: gauges.clone(),
        }
    }
}

impl Recorder for LocalGaugeRecorder {
    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {
        //  TODO: care about descriptions later.
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(Arc::new(LocalGauge::new(
            key.clone(),
            Arc::new(self.operations_sender.clone()),
        )))
    }

    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn register_counter(&self, _key: &Key, _metadata: &Metadata<'_>) -> Counter {
        Counter::noop()
    }
    fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

struct LocalGauge {
    key: Key,
    state: Arc<mpsc::Sender<GaugeOperation>>,
}

impl LocalGauge {
    #[must_use]
    pub fn new(key: Key, state: Arc<mpsc::Sender<GaugeOperation>>) -> Self {
        LocalGauge { key, state }
    }
}

impl GaugeFn for LocalGauge {
    fn increment(&self, value: f64) {
        println!("increment: {value}");
        std::mem::drop(
            self.state
                .send(GaugeOperation::Increment(self.key.clone(), value)),
        );
    }

    fn decrement(&self, value: f64) {
        println!("decrement: {value}");
        std::mem::drop(
            self.state
                .send(GaugeOperation::Decrement(self.key.clone(), value)),
        );
    }

    fn set(&self, value: f64) {
        println!("set: {value}");
        std::mem::drop(
            self.state
                .send(GaugeOperation::Set(self.key.clone(), value)),
        );
    }
}

#[derive(Default)]
pub struct CompositeRecorder {
    recorders: Vec<Box<dyn Recorder>>,
}

impl CompositeRecorder {
    // Constructor to create a new CompositeRecorder
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_recorders<R: Recorder + 'static>(recorders: Vec<R>) -> Self {
        let mut s = Self::new();
        for r in recorders {
            s.add_recorder(r);
        }
        s
    }

    // Method to add a recorder
    pub fn add_recorder<R: Recorder + 'static>(&mut self, recorder: R) {
        self.recorders.push(Box::new(recorder));
    }
}

impl Recorder for CompositeRecorder {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        for recorder in &self.recorders {
            recorder.describe_counter(key.clone(), unit, description.clone());
        }
    }

    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        for recorder in &self.recorders {
            recorder.describe_gauge(key.clone(), unit, description.clone());
        }
    }

    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        for recorder in &self.recorders {
            recorder.describe_histogram(key.clone(), unit, description.clone());
        }
    }

    fn register_counter(&self, key: &Key, metadata: &Metadata<'_>) -> Counter {
        let mut counters = CompositeCounter::new();
        for recorder in &self.recorders {
            counters.add_counter(recorder.register_counter(key, metadata));
        }
        Counter::from_arc(Arc::new(counters))
    }

    fn register_gauge(&self, key: &Key, metadata: &Metadata<'_>) -> Gauge {
        let mut gauges = CompositeGauge::new();
        for recorder in &self.recorders {
            gauges.add_gauge(recorder.register_gauge(key, metadata));
        }
        Gauge::from_arc(Arc::new(gauges))
    }

    fn register_histogram(&self, key: &Key, metadata: &Metadata<'_>) -> Histogram {
        let mut histograms = CompositeHistogram::new();
        for recorder in &self.recorders {
            histograms.add_histogram(recorder.register_histogram(key, metadata));
        }
        Histogram::from_arc(Arc::new(histograms))
    }
}

struct CompositeCounter {
    counters: Vec<Counter>,
}
impl CompositeCounter {
    #[must_use]
    pub fn new() -> Self {
        Self {
            counters: Vec::new(),
        }
    }

    pub fn add_counter(&mut self, counter: Counter) {
        self.counters.push(counter);
    }
}
impl CounterFn for CompositeCounter {
    fn increment(&self, value: u64) {
        for counter in &self.counters {
            counter.increment(value);
        }
    }

    fn absolute(&self, value: u64) {
        for counter in &self.counters {
            counter.absolute(value);
        }
    }
}

struct CompositeGauge {
    gauges: Vec<Gauge>,
}
impl CompositeGauge {
    pub fn new() -> Self {
        Self { gauges: Vec::new() }
    }

    pub fn add_gauge(&mut self, g: Gauge) {
        self.gauges.push(g);
    }
}

impl GaugeFn for CompositeGauge {
    fn increment(&self, value: f64) {
        for gauge in &self.gauges {
            gauge.increment(value);
        }
    }

    fn decrement(&self, value: f64) {
        for gauge in &self.gauges {
            gauge.decrement(value);
        }
    }

    fn set(&self, value: f64) {
        for gauge in &self.gauges {
            gauge.set(value);
        }
    }
}

struct CompositeHistogram {
    histograms: Vec<Histogram>,
}
impl CompositeHistogram {
    pub fn new() -> Self {
        Self {
            histograms: Vec::new(),
        }
    }

    pub fn add_histogram(&mut self, h: Histogram) {
        self.histograms.push(h);
    }
}
impl HistogramFn for CompositeHistogram {
    fn record(&self, value: f64) {
        for histogram in &self.histograms {
            histogram.record(value);
        }
    }
}
