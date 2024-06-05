use std::sync::MutexGuard;

use crate::DataFusion;

impl DataFusion {
    pub fn mark_initial_load_complete(&self) {
        let mut guard = self.get_initial_load_guard();
        *guard = true;
    }

    pub fn is_initial_load_complete(&self) -> bool {
        let guard = self.get_initial_load_guard();
        *guard
    }

    fn get_initial_load_guard(&self) -> MutexGuard<bool> {
        match self.initial_load_complete.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}
