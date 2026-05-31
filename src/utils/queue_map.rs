use std::hash::Hash;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tracing::error;

/// A map of queues that guarantees at most exactly one receiver for each key can exist at one time.
///
/// Elements arrive to a receiver strictly in the order they are enqueued.
pub struct QueueMap<K, V> {
    map: Arc<DashMap<K, mpsc::UnboundedSender<V>>>,
}

impl<K, V> QueueMap<K, V>
where
    K: Eq + Hash,
{
    /// Create an empty QueueMap.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<K, V> QueueMap<K, V>
where
    K: Eq + Hash + Clone,
{
    /// Push a new value into the Queue with the given key, creating a new queue and returning a
    /// receiver if the queue did not yet exist.
    #[must_use = "not handling an QueueMapReceiver will cause the enqueued items to never be handled"]
    pub fn enqueue(&self, key: K, mut value: V) -> Option<QueueMapReceiver<K, V>> {
        loop {
            // Try to first send the event with just a read lock on the channel, if it exists.
            // Reduces the chance a write lock needs to be held.
            if let Some(sender) = self.map.get(&key) {
                match sender.send(value) {
                    Ok(()) => {
                        return None;
                    }
                    Err(SendError(returned_event)) => {
                        // Drop the sender to avoid a race condition where the code below tries to
                        // mutably access the shard but cant because we still have a lock on it
                        // here.
                        drop(sender);

                        self.map.remove_if(&key, |_, v| v.is_closed());
                        value = returned_event;
                    }
                }
            }

            let mut new_receiver = None;

            // Use a block to drop the lock as soon as we can.
            let send_result = {
                let sender = self.map.entry(key.clone()).or_insert_with(|| {
                    let (send, recv) = mpsc::unbounded_channel();

                    new_receiver = Some(recv);
                    send
                });

                // Send the event while still holding a lock over the shard. Without a lock this
                // channel may be dropped and the event would never be handled.
                sender.send(value)
            };

            match send_result {
                Ok(()) => {
                    return new_receiver.map(|receiver| QueueMapReceiver {
                        key,
                        receiver,
                        queue: self.clone(),
                        finished: false,
                    });
                }
                Err(SendError(returned_event)) => {
                    // This scenario is possible if an QueueMapReceiver was dropped. In this case,
                    // try to remove the channel from the map but make sure not to remove it if
                    // somewhere else a new channel was already inserted.
                    self.map.remove_if(&key, |_, v| v.is_closed());
                    value = returned_event;
                }
            }
        }
    }
}

impl<K, V> Clone for QueueMap<K, V> {
    /// Create a new reference to the same QueueMap.
    fn clone(&self) -> Self {
        Self {
            map: self.map.clone(),
        }
    }
}

impl<K, V> Default for QueueMap<K, V>
where
    K: Eq + Hash,
{
    fn default() -> Self {
        Self {
            map: Default::default(),
        }
    }
}

/// Receiver for a specific key in a [QueueMap].
///
/// # Cancel Safety
///
/// All methods are cancel safe.
///
/// # Dropping
///
/// A QueueMapReceiver should never be dropped. Instead, use
/// [try_finish](QueueMapReceiver::try_finish) in order to ensure no new values have been enqueued
/// before closing the receiver.
///
/// If the receiver is dropped anyway, any remaining values are also dropped and the key is removed
/// from the QueueMap.
#[must_use = "QueueMapReceiver still may hold values unless try_finish succeeds"]
pub struct QueueMapReceiver<K, V>
where
    K: Eq + Hash,
{
    key: K,
    receiver: mpsc::UnboundedReceiver<V>,
    queue: QueueMap<K, V>,
    finished: bool,
}

impl<K, V> QueueMapReceiver<K, V>
where
    K: Eq + Hash,
{
    /// Return the key in the queue that this receiver will receive items for.
    pub fn key(&self) -> &K {
        &self.key
    }

    /// Tries to finish and close the receiver, removing it from the queue mapping.
    ///
    /// Fails if there are any unhandled items in the receiver.
    #[must_use = "dropping an unfinished QueueMapReceiver is a race condition"]
    pub fn try_finish(mut self) -> Result<(), Self> {
        let Some((_, _removed_sender)) = self
            .queue
            .map
            .remove_if(&self.key, |_, _sender| self.receiver.is_empty())
        else {
            return Err(self);
        };

        if !self.receiver.is_empty() {
            // This race condition should never be able to occur as the only way to write to this
            // channel's sender half is to have a lock on the map.
            error!("race condition detected in QueueMapReceiver");
        }
        self.receiver.close();
        self.finished = true;
        Ok(())
    }

    /// Return the amount of unhandled items currently in the queue.
    pub fn len(&self) -> usize {
        self.receiver.len()
    }

    /// Returns true if the queue is currently empty.
    pub fn is_empty(&self) -> bool {
        self.receiver.is_empty()
    }

    /// Wait for the next value in the queue.
    pub async fn recv(&mut self) -> V {
        self.receiver
            .recv()
            .await
            .expect("channel can only be closed on drop")
    }

    /// Wait for the next value in the queue. If multiple values have accumulated, at most `limit`
    /// values can be written out to `buffer`.
    ///
    /// Returns the number of values received.
    pub async fn recv_many(&mut self, buffer: &mut Vec<V>, limit: usize) -> usize {
        self.receiver.recv_many(buffer, limit).await
    }
}

impl<K, V> Drop for QueueMapReceiver<K, V>
where
    K: Eq + Hash,
{
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // Close the sender, and remove it from the map only if no other sender has been put in its
        // place between these two operations.
        self.receiver.close();
        self.queue
            .map
            .remove_if(&self.key, |_, sender| sender.is_closed());
    }
}
