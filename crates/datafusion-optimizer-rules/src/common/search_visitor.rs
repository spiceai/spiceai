use datafusion::common::Result;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

#[macro_export]
macro_rules! concrete {
    ($var:expr, $t:ty) => {
        $var.as_ref().as_any().downcast_ref::<$t>()
    };
}

/// A generalized `TreeNodeVisitor` that collects values from the provided fns during traversal
#[allow(clippy::type_complexity)]
pub struct SearchVisitor<T> {
    pub values: Vec<T>,
    pub limit: Option<usize>,
    f_up: Option<Box<dyn FnMut(&Arc<dyn ExecutionPlan>) -> Option<T> + 'static>>,
    f_down: Option<Box<dyn FnMut(&Arc<dyn ExecutionPlan>) -> Option<T> + 'static>>,
}

impl<T> Default for SearchVisitor<T> {
    fn default() -> Self {
        Self {
            values: vec![],
            limit: None,
            f_up: None,
            f_down: None,
        }
    }
}

impl SearchVisitor<()> {
    /// # Errors
    /// Returns an error if the plan visitor encounters an error during traversal
    pub fn vec_down(plan: &Arc<dyn ExecutionPlan>) -> Result<Vec<Arc<dyn ExecutionPlan>>> {
        SearchVisitor::default()
            .down(|p| Some(Arc::clone(p)))
            .find(plan)
    }

    /// # Errors
    /// Returns an error if the plan visitor encounters an error during traversal
    pub fn collect_concrete_down<C: ExecutionPlan + 'static>(
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<Arc<dyn ExecutionPlan>>> {
        SearchVisitor::default()
            .down(Self::arc_if_concrete::<C>)
            .find(plan)
    }

    /// # Errors
    /// Returns an error if the plan visitor encounters an error during traversal
    pub fn collect_concrete_up<C: ExecutionPlan + 'static>(
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<Arc<dyn ExecutionPlan>>> {
        SearchVisitor::default()
            .up(Self::arc_if_concrete::<C>)
            .find(plan)
    }

    /// # Errors
    /// Returns an error if the plan visitor encounters an error during traversal
    pub fn first_concrete_down<C: ExecutionPlan + 'static>(
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        SearchVisitor::default()
            .down(Self::arc_if_concrete::<C>)
            .find_first(plan)
    }

    /// # Errors
    /// Returns an error if the plan visitor encounters an error during traversal
    pub fn first_concrete_up<C: ExecutionPlan + 'static>(
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        SearchVisitor::default()
            .up(Self::arc_if_concrete::<C>)
            .find_first(plan)
    }

    fn arc_if_concrete<C: ExecutionPlan + Send + Sync + 'static>(
        node: &Arc<dyn ExecutionPlan>,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        // Arc::downcast requires Send/Sync, but ExecutionPlan implements those
        if node.as_ref().as_any().is::<C>() {
            Some(Arc::clone(node))
        } else {
            None
        }
    }
}

impl<T: 'static> SearchVisitor<T> {
    // Builder
    #[must_use]
    pub fn up(mut self, func: impl FnMut(&Arc<dyn ExecutionPlan>) -> Option<T> + 'static) -> Self {
        self.f_up = Some(Box::new(func));
        self
    }

    #[must_use]
    pub fn down(
        mut self,
        func: impl FnMut(&Arc<dyn ExecutionPlan>) -> Option<T> + 'static,
    ) -> Self {
        self.f_down = Some(Box::new(func));
        self
    }

    #[must_use]
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    // API

    /// # Errors
    /// Returns an error if the plan visitor encounters an error during traversal
    pub fn find(mut self, plan: &Arc<dyn ExecutionPlan>) -> Result<Vec<T>> {
        plan.visit(&mut self)?;
        Ok(self.values)
    }

    /// # Errors
    /// Returns an error if the plan visitor encounters an error during traversal
    pub fn find_first(self, plan: &Arc<dyn ExecutionPlan>) -> Result<Option<T>> {
        let mut with_limit = self.limit(1);
        plan.visit(&mut with_limit)?;
        Ok(with_limit.values.into_iter().next())
    }

    fn invoke_up(&mut self, plan: &Arc<dyn ExecutionPlan>) -> Option<T> {
        if let Some(f) = self.f_up.as_mut() {
            f(plan)
        } else {
            None
        }
    }

    fn invoke_down(&mut self, plan: &Arc<dyn ExecutionPlan>) -> Option<T> {
        if let Some(f) = self.f_down.as_mut() {
            f(plan)
        } else {
            None
        }
    }
}

impl<T: 'static> TreeNodeVisitor<'_> for SearchVisitor<T> {
    type Node = Arc<dyn ExecutionPlan>;
    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        if let Some(limit) = self.limit
            && self.values.len() > limit
        {
            return Ok(TreeNodeRecursion::Stop);
        }

        if let Some(found) = self.invoke_down(node) {
            self.values.push(found);
        }

        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        if let Some(limit) = self.limit
            && self.values.len() > limit
        {
            return Ok(TreeNodeRecursion::Stop);
        }

        if let Some(found) = self.invoke_up(node) {
            self.values.push(found);
        }

        Ok(TreeNodeRecursion::Continue)
    }
}
