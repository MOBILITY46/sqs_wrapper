extern crate futures;
extern crate log;
extern crate rusoto_core;
extern crate rusoto_sqs;

use futures::{Async, Future, Poll, Stream};
use log::{info, trace};
use rusoto_core::{Region, RusotoFuture};
use rusoto_sqs::{self as sqs, Sqs};
use serde::de::DeserializeOwned;
#[allow(unused_imports)]
use std::{collections::HashMap, error::Error as StdError, str::FromStr};

mod error;
mod queue_settings;

pub use error::Error;

pub use crate::queue_settings::QueueSettings;
pub use rusoto_sqs::ReceiveMessageError;

#[derive(Debug)]
pub struct QueueItem<T>
where
    T: DeserializeOwned + Send + 'static,
{
    pub message_id: String,
    pub receipt_handle: String,
    pub attributes: Option<HashMap<String, String>>,
    pub body: Result<T, serde_json::Error>,
}

impl<T> QueueItem<T>
where
    T: DeserializeOwned + Send + 'static,
{
    fn try_from(msg: sqs::Message) -> Option<QueueItem<T>> {
        let msg_id = msg
            .message_id
            .ok_or_else(|| info!("Dropping message without message_id"))
            .ok()?;
        let receipt_handle = msg
            .receipt_handle
            .ok_or_else(|| info!("Dropping message({:?}) without receipt_handle", msg_id))
            .ok()?;
        let body = msg
            .body
            .ok_or_else(|| info!("Dropping message({:?}) without body", msg_id))
            .ok()?;

        let queue_item = QueueItem {
            message_id: msg_id,
            receipt_handle: receipt_handle,
            attributes: msg.attributes,
            body: serde_json::from_str(&body),
        };

        Some(queue_item)
    }
}

pub struct RecordStream<T>
where
    T: DeserializeOwned + Send + 'static,
{
    queue: Queue<T>,
    future: Box<dyn Future<Item = Vec<QueueItem<T>>, Error = Error> + Send + 'static>,
    message_buffer: Vec<QueueItem<T>>,
    wait_time_seconds: i64,
}

impl<T> Stream for RecordStream<T>
where
    T: DeserializeOwned,
    T: Send + 'static,
{
    type Item = QueueItem<T>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if !self.message_buffer.is_empty() {
            return Ok(Async::Ready(Some(self.message_buffer.remove(0))));
        }

        match self.future.poll() {
            Ok(Async::NotReady) => {
                return Ok(Async::NotReady);
            }
            Err(err) => {
                return Err(err);
            }
            Ok(Async::Ready(messages)) => {
                // For now we just drop all messages that could not be parsed. But we print the reason.
                messages
                    .into_iter()
                    .for_each(|msg| self.message_buffer.push(msg))
                //     res.messages
                //         .into_iter()
                //         .flatten()
                //         .filter_map(|msg: sqs::Message| QueueItem::try_from(msg))
                //         .filter_map(|opt_item| opt_item.ok())
                //         .for_each(|msg: QueueItem<T>| self.message_buffer.push(msg));
            }
        }

        trace!("Polling queue");
        self.future = Box::new(self.queue.receive_messages_async(self.wait_time_seconds));
        self.poll()
    }
}

pub struct Queue<T>
where
    T: Send + 'static,
{
    pub queue_url: String,
    pub region: Region,
    c: sqs::SqsClient,

    _phantom_data: std::marker::PhantomData<T>,
}

/// Try to convert the example in wait_and_read_one to use futures and Tokio.
impl<T> Queue<T>
where
    T: serde::de::DeserializeOwned + Send + 'static,
{
    pub fn connect(region: Region, queue_url: String) -> Self {
        Queue {
            c: sqs::SqsClient::new(region.clone()),
            region: region.clone(),
            queue_url: queue_url,
            _phantom_data: std::marker::PhantomData,
        }
    }

    pub fn create(
        region: Region,
        name: String,
        settings: QueueSettings,
    ) -> impl Future<Item = Queue<T>, Error = sqs::CreateQueueError> {
        let mut attrs: HashMap<String, String> = HashMap::new();

        if let Some(secs) = settings.delay_seconds {
            attrs.insert("DelaySeconds".into(), format!("{}", secs));
        }

        if let Some(bytes) = settings.max_size_bytes {
            attrs.insert("MaximumMessageSize".into(), format!("{}", bytes));
        }

        if let Some(secs) = settings.message_retention_period_seconds {
            attrs.insert("MessageRetentionPeriod".into(), format!("{}", secs));
        }

        if let Some(secs) = settings.receive_wait_time_seconds {
            attrs.insert("ReceiveMessageWaitTimeSeconds".into(), format!("{}", secs));
        }

        if let Some(secs) = settings.visibility_timeout_seconds {
            attrs.insert("VisibilityTimeout".into(), format!("{}", secs));
        }

        let req = sqs::CreateQueueRequest {
            queue_name: name,
            attributes: Some(attrs),
        };

        let client = sqs::SqsClient::new(region.clone());
        client.create_queue(req).map(move |res| Queue {
            queue_url: res.queue_url.unwrap(),
            region: region,
            c: client,
            _phantom_data: std::marker::PhantomData,
        })
    }

    pub fn purge(&self) -> RusotoFuture<(), sqs::PurgeQueueError> {
        let req = sqs::PurgeQueueRequest {
            queue_url: self.queue_url.clone(),
        };

        self.c.purge_queue(req)
    }

    pub(crate) fn receive_messages_async(
        &self,
        wait_time_seconds: i64,
    ) -> impl Future<Item = Vec<QueueItem<T>>, Error = Error> + Send + Sized + 'static {
        let msg = sqs::ReceiveMessageRequest {
            attribute_names: Some(vec!["ALL".into()]),
            max_number_of_messages: None,
            message_attribute_names: Some(vec!["ALL".into()]),
            queue_url: self.queue_url.clone(),
            receive_request_attempt_id: None, // FIFO ONLY
            visibility_timeout: None,
            wait_time_seconds: Some(wait_time_seconds),
        };

        self.c
            .receive_message(msg)
            .map_err(Error::from)
            .map(|sqs_msg| {
                sqs_msg
                    .messages
                    .into_iter()
                    .flatten()
                    .filter_map(|msg: sqs::Message| QueueItem::try_from(msg))
                    .collect::<Vec<_>>()
            })
    }

    pub fn receive_messages_sync(
        &self,
        wait_time_seconds: i64,
    ) -> Result<Vec<QueueItem<T>>, Error> {
        let msg = sqs::ReceiveMessageRequest {
            attribute_names: Some(vec!["ALL".into()]),
            max_number_of_messages: None,
            message_attribute_names: Some(vec!["ALL".into()]),
            queue_url: self.queue_url.clone(),
            receive_request_attempt_id: None, // FIFO ONLY
            visibility_timeout: None,
            wait_time_seconds: Some(wait_time_seconds),
        };

        self.c
            .receive_message(msg)
            .sync()
            .map_err(Error::from)
            .map(|sqs_msg| {
                sqs_msg
                    .messages
                    .into_iter()
                    .flatten()
                    .filter_map(|msg: sqs::Message| QueueItem::try_from(msg))
                    .collect::<Vec<_>>()
            })
    }

    pub fn delete_message(
        &self,
        receipt_handle: String,
    ) -> RusotoFuture<(), sqs::DeleteMessageError> {
        let msg = sqs::DeleteMessageRequest {
            queue_url: self.queue_url.clone(),
            receipt_handle: receipt_handle,
        };

        self.c.delete_message(msg)
    }

    pub fn stream_messages(&self, wait_time_seconds: i64) -> RecordStream<T> {
        let initial_future = self.receive_messages_async(wait_time_seconds);

        RecordStream {
            queue: Queue {
                queue_url: self.queue_url.clone(),
                region: self.region.clone(),
                c: sqs::SqsClient::new(self.region.clone()),
                _phantom_data: std::marker::PhantomData,
            },
            future: Box::new(initial_future),
            message_buffer: Vec::with_capacity(10),
            wait_time_seconds: wait_time_seconds,
        }
    }
}

impl<T> Queue<T>
where
    T: serde::Serialize + Send + 'static,
{
    pub fn enqueue_message(
        &self,
        payload: &T,
    ) -> RusotoFuture<sqs::SendMessageResult, sqs::SendMessageError> {
        let serialized_payload = serde_json::to_string(payload).expect("Serializing payload");

        let req = sqs::SendMessageRequest {
            delay_seconds: None,
            message_attributes: None,
            message_deduplication_id: None,
            message_group_id: None,
            queue_url: self.queue_url.clone(),
            message_body: serialized_payload,
        };

        self.c.send_message(req)
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;
    use std::time::SystemTime;
    use tokio::runtime::Runtime;

    use serde_derive::{Deserialize, Serialize};

    use super::futures::future;
    use super::*;

    #[derive(Deserialize, Serialize)]
    struct TestItem {
        message: String,
    }

    fn with_queue<F>(f: F)
    where
        F: FnOnce((Arc<Queue<TestItem>>, &mut Runtime)) -> (),
    {
        let queue = Queue::<TestItem>::connect(
            Region::EuCentral1,
            "https://sqs.eu-central-1.amazonaws.com/396885474243/sqs_wrapper_test_queue".into(),
        );

        let rt = Runtime::new();
        assert!(rt.is_ok(), "Runtime failed to initialize");
        let mut rt = rt.unwrap();

        f((Arc::new(queue), &mut rt));
    }

    #[test]
    fn test_enqueue_and_delete_message() {
        with_queue(|(queue, rt)| {
            let payload = serde_json::to_string(&TestItem {
                message: "herpderp".into(),
            })
            .unwrap();

            // ENQUEUE
            let enq_res = rt.block_on(queue.enqueue_message(payload.clone()));

            let enq_res = match enq_res {
                Ok(res) => res,

                Err(rusoto_sqs::SendMessageError::Unknown(raw)) => panic!(
                    "Enqueing message failed:\n{}",
                    std::str::from_utf8(&raw.body).unwrap()
                ),

                Err(e) => panic!("Enqueing message failed: {}", e),
            };

            assert!(enq_res.message_id.is_some(), "Message ID was None");

            let message_id = enq_res.message_id.unwrap();
            assert!(0 < message_id.len(), "Message ID was empty string");

            // DEQUEUE
            let deq_res = rt.block_on(queue.receive_messages(10));
            assert!(deq_res.is_ok(), "Error receiving messages");

            let deq_messages = deq_res.unwrap();
            assert!(0 < deq_messages.len(), "Received no messages");

            let deq_message = deq_messages.get(0).unwrap();

            // DELETE
            let receipt_handle = deq_message.receipt_handle.clone();
            assert!(0 < receipt_handle.len(), "Receipt handle was empty string");

            let delete_res = rt.block_on(queue.delete_message(receipt_handle));

            assert!(delete_res.is_ok(), "Error deleting the message");
        });
    }

    #[test]
    fn test_message_stream() {
        with_queue(|(queue, rt)| {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap();

            let msgs_to_send = 10;

            let enqueued = {
                let now = (now.as_secs() * 1000) + now.subsec_millis() as u64;
                let queue = queue.clone();
                // push some messages
                let to_send = (1..=msgs_to_send)
                    .map(move |i| {
                        let msg = format!(
                            "{{ \"payload\": \"Stream test {}\", \"timestamp\": \"{}\"}}",
                            i, now
                        );

                        serde_json::to_string(&TestItem { message: msg }).unwrap()
                    })
                    .map(move |payload| queue.clone().enqueue_message(payload));

                let enqueued = rt.block_on(future::join_all(to_send));

                assert!(
                    enqueued.is_ok(),
                    ("Error enqueueing messages: {}", enqueued.err())
                );
                enqueued.unwrap()
            };

            assert_eq!(
                msgs_to_send,
                enqueued.len(),
                "All enqueued messages were not successful"
            );

            let streamed_res = queue
                .clone()
                .stream_messages(5)
                .take(msgs_to_send as u64)
                .collect();

            match rt.block_on(streamed_res) {
                Ok(_) => (),
                Err(e) => panic!("Stream got error: {:?}", e),
            }
        });
    }

}
