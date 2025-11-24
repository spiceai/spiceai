#[derive(Clone, Debug)]
pub struct ApiShard {
    pub shard_id: String,
    pub parent_shard_id: Option<String>,
    pub starting_sequence_number: Option<String>,

    // None = still open
    pub ending_sequence_number: Option<String>,
}

impl ApiShard {
    pub fn id(&self) -> &str {
        &self.shard_id
    }

    pub fn parent_id(&self) -> Option<&str> {
        self.parent_shard_id.as_deref()
    }
}
