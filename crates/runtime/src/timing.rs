use std::time::Instant;

/// `measure_scope!` measures the time for which the designated scope lives. 
///
/// ## Usage
///   - Example, measure a function's whole duration:
///   ```
///   fn my_function() {
///     measure_scope!("process_data");
///     sleep(Duration::from_secs(1))
///   
///   } // 'process_data' measures until the end of the function scope (via implementing `Drop`).
///   ```
///
///   - Example, measure a specific scope
///   ```
///   fn my_function() {
///     // Some work
///     sleep(Duration::from_secs(1))
///     {
///         // Some work we don't want to measure
///         let x = 1+2;
/// 
///         // Some work we want to measure
///         measure_scope!("process_data");
///         let y = 2*3;
///         sleep(Duration::from_secs(1))
///     } // 'process_data' duration ends here.
///   } 
///   ```
///   - **Example**: Add properties to the measurement (key `&str`, value `ToString`)
///   ```
///   fn my_function(x: int, y: String) {
///     measure_scope!("process_data", "x" => x, "y" => y);
///   }
///   ```
/// ## Parameters
///
/// - `$name:expr` — A string literal representing the name of the scope being measured.
/// - `$key:expr => $value:expr` — Optional key-value pairs provided as additional metadata
///   for the timing measurement.
///
/// ```
#[macro_export]
macro_rules! measure_scope {
    ($name:expr, $($key:expr => $value:expr),+ $(,)?) => {
        let args = vec![$(($key, $value.to_string())),+];
        $crate::timing::TimingGuard::new($name, args)
    };
    ($name:expr) => {
        $crate::timing::TimingGuard::new($name, vec![])
    };
}

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
        metrics::histogram!(self.metric_name, &self.labels).record(self.start.elapsed().as_secs_f64()); // .as_millis() as f64
    }
}