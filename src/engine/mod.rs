pub mod api;
pub mod iter;
pub mod persistence;

pub use iter::OrbyIterator;

#[cfg(test)]
mod tests;

use crate::builder::OrbyBuilder;
use crate::error::OrbyError;
use crate::logic::OrbyRingBufferSilo;
use crate::types::{LogicMode, SaveMode};
use parking_lot::RwLock;
use std::sync::Arc;
use sysinfo::System;

/// High-performance in-memory index engine specialized for 128-bit fixed-length data scanning.
#[derive(Clone)]
pub struct Orby {
    pub(crate) inner: Arc<RwLock<OrbyRingBufferSilo>>,
}

impl Orby {
    /// Creates a new `OrbyBuilder` with the specified label.
    ///
    /// # Examples
    ///
    /// ```
    /// use orby::Orby;
    /// let builder = Orby::builder("my_ring");
    /// ```
    pub fn builder(label: &str) -> OrbyBuilder {
        OrbyBuilder::new(label)
    }

    /// Initializes a new `Orby` instance with primary configuration.
    ///
    /// # Examples
    ///
    /// ```
    /// use orby::{Orby, SaveMode, LogicMode};
    ///
    /// # async fn example() -> Result<(), orby::error::OrbyError> {
    /// let orby = Orby::new(
    ///     "example_ring",
    ///     1000,
    ///     2,
    ///     SaveMode::MemoryOnly,
    ///     LogicMode::RingBuffer,
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(
        label: &str,
        ring_buffer_lane_item_count: usize,
        ring_buffer_lane_count: usize,
        storage_mode: SaveMode,
        logic_mode: LogicMode,
    ) -> Result<Self, OrbyError> {
        Self::builder(label)
            .ring_buffer_lane_item_count(ring_buffer_lane_item_count)
            .ring_buffer_lane_count(ring_buffer_lane_count)
            .with_storage(storage_mode)
            .logic_mode(logic_mode)
            .build()
            .await
    }

    /// Returns the metadata of the store.
    /// Returns: (current_length, capacity, lane_count)
    pub fn meta(&self) -> (usize, usize, usize) {
        let store = self.inner.read();
        (store.len, store.capacity, store.ring_buffer_lane_count)
    }

    pub(crate) async fn try_new(
        name: &str,
        capacity: usize,
        ring_buffer_lane_count: usize,
        storage_mode: SaveMode,
        logic_mode: LogicMode,
        compaction: bool,
        aof_enabled: bool,
        capacity_usage_ratio: f64,
    ) -> Result<Self, OrbyError> {
        let row_bytes = ring_buffer_lane_count * 16;
        let required_bytes = capacity as u64 * row_bytes as u64;

        if required_bytes > 0 {
            let mut sys = System::new_all();
            sys.refresh_memory();
            let available_bytes = sys.available_memory();
            let safety_limit = (available_bytes as f64 * capacity_usage_ratio) as u64;

            if safety_limit > 0 && required_bytes > safety_limit {
                return Err(OrbyError::InsufficientMemory {
                    requested_mb: required_bytes / 1024 / 1024,
                    available_mb: available_bytes / 1024 / 1024,
                });
            }
        }

        let mut aof_sender = None;
        let mirror_sender = None;
        let mut mirror_path_buf = None;
        let mut vault_path_buf = None;

        let (resolved_mirror_path, resolved_aof_path) = match &storage_mode {
            SaveMode::Vault(opt_path) => {
                let base_path = opt_path
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| std::path::PathBuf::from("db_data"));
                let vault_dir = base_path.join(name);
                vault_path_buf = Some(vault_dir);
                (None, None)
            }
            _ => (None, None),
        };

        if mirror_path_buf.is_none() {
            mirror_path_buf = resolved_mirror_path.clone();
        }

        if aof_enabled {
            let aof_path = resolved_aof_path
                .clone()
                .unwrap_or_else(|| std::path::PathBuf::from(format!("{}.aof", name)));
            let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1024);
            Self::spawn_aof_worker(name.to_string(), aof_path, rx);
            aof_sender = Some(tx);
        }

        use crate::logic::OrbyRingBuffer;
        let lanes = (0..ring_buffer_lane_count)
            .map(|_| OrbyRingBuffer::new(capacity))
            .collect();

        Ok(Self {
            inner: Arc::new(RwLock::new(OrbyRingBufferSilo {
                name: name.to_string(),
                lanes,
                cursor: 0,
                len: 0,
                capacity,
                ring_buffer_lane_count,
                compaction,
                logic_mode,
                storage_mode,
                aof_sender,
                mirror_sender,
                mirror_path: mirror_path_buf,
                vault_path: vault_path_buf,
            })),
        })
    }

    /// Returns the name of the ring buffer.
    pub fn name(&self) -> String {
        self.inner.read().name.clone()
    }
    /// Returns the current number of elements in the ring buffer.
    pub fn len(&self) -> usize {
        self.inner.read().len
    }
    /// Returns the count of active cells (non-zero entries) in the primary lane.
    pub fn count_active(&self) -> usize {
        let store = self.inner.read();
        crate::logic::ring::count_active(&store)
    }
    /// Returns true if the ring buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.read().len == 0
    }
    /// Deletes the data at the specified index.
    /// Returns true if the deletion was successful.
    pub async fn delete(&self, index: usize) -> bool {
        let (res, has_vault, compaction) = {
            let mut store = self.inner.write();
            let (res, changes) = crate::logic::ring::delete(&mut *store, index);
            let has_vault = store.vault_path.is_some();
            let compaction = store.compaction;
            self.dispatch_persistence(&mut *store, changes);
            (res, has_vault, compaction)
        };

        if res && has_vault {
            if compaction {
                let _ = self.vault_delete_and_compact(index).await;
            } else {
                // Sparse delete: zero out entry in Vault
                let dim = self.meta().2;
                let zeros = vec![vec![0u128; dim]];
                let _ = self.commit_vault_batch(index, zeros).await;
            }
        }
        res
    }

    fn dispatch_persistence(
        &self,
        store: &mut OrbyRingBufferSilo,
        changes: crate::logic::PersistenceChanges,
    ) {
        let (aof_data, mirror_data) = changes.flatten(store.ring_buffer_lane_count);
        if let Some(sender) = &store.aof_sender {
            if !aof_data.is_empty() {
                let _ = sender.try_send(aof_data);
            }
        }
        if let Some(sender) = &store.mirror_sender {
            if !mirror_data.is_empty() {
                let _ = sender.try_send(mirror_data);
            }
        }
    }
}
