use std::time::Instant;

pub struct TimingGuard {
    start: Instant,
    metric_name: &'static str,
    labels: Vec<(&'static str, String)>
}

impl TimingGuard {
    pub fn new(metric_name: &'static str, labels: Vec<(&'static str, String)>) -> Self {
        Self {
            start: Instant::now(),
            metric_name,
            labels
        }
    }
}

impl Drop for TimingGuard {
    fn drop(&mut self) {
        metrics::histogram!(self.metric_name, &self.labels).record(self.start.elapsed().as_secs_f64());
    }
}