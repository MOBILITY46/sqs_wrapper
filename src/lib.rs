extern crate futures;
extern crate log;
extern crate rusoto_core;
extern crate rusoto_sqs;

use std::collections::HashMap;

use futures::{Async, Future, Poll, Stream};
use rusoto_core::{Region, RusotoFuture};
use rusoto_sqs::{self as sqs, Sqs};

mod queue_settings;

pub use queue_settings::QueueSettings;

pub struct RecordStream {
    queue: Queue,
    future: RusotoFuture<sqs::ReceiveMessageResult, sqs::ReceiveMessageError>,
    message_buffer: Vec<sqs::Message>,
}

impl Stream for RecordStream {
    type Item = sqs::Message;
    type Error = sqs::ReceiveMessageError;

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
            Ok(Async::Ready(res)) => {
                // Request more
                match res.messages {
                    Some(messages) => {
                        messages
                            .into_iter()
                            .for_each(|msg| self.message_buffer.push(msg));
                    }

                    None => (),
                }

                self.future = self.queue.receive_messages();
                self.poll()
            }
        }
    }
}

pub struct Queue {
    queue_url: String,
    region: Region,
    c: sqs::SqsClient,
}

/// Try to convert the example in wait_and_read_one to use futures and Tokio.
impl Queue {
    pub fn connect<S: Into<String>>(region: Region, queue_url: S) -> Self {
        Queue {
            c: sqs::SqsClient::new(region.clone()),
            region: region.clone(),
            queue_url: queue_url.into(),
        }
    }

    pub fn create(
        region: Region,
        name: String,
        settings: QueueSettings,
    ) -> impl Future<Item = Queue, Error = sqs::CreateQueueError> {
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
        })
    }

    pub fn purge(&self) -> RusotoFuture<(), sqs::PurgeQueueError> {
        let req = sqs::PurgeQueueRequest {
            queue_url: self.queue_url.clone(),
        };

        self.c.purge_queue(req)
    }

    pub fn enqueue_message(
        &self,
        payload: String,
    ) -> RusotoFuture<sqs::SendMessageResult, sqs::SendMessageError> {
        let req = sqs::SendMessageRequest {
            delay_seconds: None,
            message_attributes: None,
            message_deduplication_id: None,
            message_group_id: None,
            queue_url: self.queue_url.clone(),
            message_body: payload,
        };

        self.c.send_message(req)
    }

    pub fn receive_messages(
        &self,
    ) -> RusotoFuture<sqs::ReceiveMessageResult, sqs::ReceiveMessageError> {
        let msg = sqs::ReceiveMessageRequest {
            attribute_names: Some(vec!["ALL".into()]),
            max_number_of_messages: None,
            message_attribute_names: Some(vec!["ALL".into()]),
            queue_url: self.queue_url.clone(),
            receive_request_attempt_id: None, // FIFO ONLY
            visibility_timeout: None,
            wait_time_seconds: None,
        };

        self.c.receive_message(msg)
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

    pub fn stream_messages(&self) -> RecordStream {
        let initial_future = self.receive_messages();

        RecordStream {
            queue: Queue {
                queue_url: self.queue_url.clone(),
                region: self.region.clone(),
                c: sqs::SqsClient::new(self.region.clone()),
            },
            future: initial_future,
            message_buffer: Vec::with_capacity(10),
        }
    }
}

#[cfg(test)]
mod tests {

    extern crate tokio;

    use self::tokio::runtime::Runtime;
    use std::sync::Arc;
    use std::time::SystemTime;

    use super::futures::future;
    use super::*;

    fn with_queue<F>(test_name: &'static str, f: F)
    where
        F: FnOnce((Arc<Queue>, &mut Runtime)) -> (),
    {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();

        let now = (now.as_secs() * 1000) + now.subsec_millis() as u64;
        let queue_name = format!("sqs-wrapper-test-{}-{}", test_name, now);

        let mut settings = QueueSettings::default();
        settings.delay_seconds = Some(0);

        let queue_f = Queue::create(Region::EuCentral1, queue_name, settings);

        let rt = Runtime::new();
        assert!(rt.is_ok(), "Runtime failed to initialize");
        let mut rt = rt.unwrap();

        let queue = rt.block_on(queue_f);
        assert!(queue.is_ok(), "Failed to create queue");
        let queue = queue.unwrap();

        let queue_url = queue.queue_url.clone();
        let region = queue.region.clone();

        f((Arc::new(queue), &mut rt));

        delete_queue(region, queue_url, &mut rt);
    }

    fn delete_queue(region: Region, queue_url: String, rt: &mut Runtime) {
        let client = sqs::SqsClient::new(region.clone());

        let delete_res = rt.block_on(client.delete_queue(sqs::DeleteQueueRequest {
            queue_url: queue_url,
        }));
        assert!(
            delete_res.is_ok(),
            "Failed to delete queue: {:?}",
            delete_res.err()
        );
    }

    #[test]
    fn test_enqueue_and_delete_message() {
        with_queue("simple-enqueue", |(queue, rt)| {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap();

            let payload = format!(
                "{{ \"payload\": \"My cool payload used for testing\", \"timestamp\": \"{}.{}\"}}",
                now.as_secs(),
                now.subsec_millis(),
            );

            // ENQUEUE
            let enq_res = rt.block_on(queue.enqueue_message(payload.clone()));
            assert!(enq_res.is_ok(), "Error enqueuing message");

            let enq_res = enq_res.unwrap();
            assert!(enq_res.message_id.is_some(), "Message ID was None");

            let message_id = enq_res.message_id.unwrap();
            assert!(0 < message_id.len(), "Message ID was empty string");

            // DEQUEUE
            let deq_res = rt.block_on(queue.receive_messages());
            assert!(deq_res.is_ok(), "Error receiving messages");

            let deq_res = deq_res.unwrap();
            assert!(deq_res.messages.is_some(), "Messages was None");
            let deq_messages = deq_res.messages.unwrap();

            assert!(0 < deq_messages.len(), "Received no messages");
            let deq_message = deq_messages.get(0).unwrap();
            assert!(deq_message.body.is_some(), "Dequeued message body was None");
            assert_eq!(deq_message.body, Some(payload));

            // DELETE
            let receipt_handle = deq_message.receipt_handle.clone();
            assert!(
                receipt_handle.is_some(),
                "Receipt handle was None, cannot delete!"
            );

            let receipt_handle = receipt_handle.unwrap();
            assert!(0 < receipt_handle.len(), "Receipt handle was empty string");

            let delete_res = rt.block_on(queue.delete_message(receipt_handle));

            assert!(delete_res.is_ok(), "Error deleting the message");
        });
    }

    #[test]
    fn test_message_stream() {
        with_queue("streaming", |(queue, rt)| {
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
                        format!(
                            "{{ \"payload\": \"Stream test {}\", \"timestamp\": \"{}\"}}",
                            i, now
                        )
                    })
                    .map(move |payload| queue.clone().enqueue_message(payload));

                let enqueued = rt.block_on(future::join_all(to_send));

                assert!(enqueued.is_ok(), "Error enqueueing messages");
                enqueued.unwrap()
            };

            assert_eq!(
                msgs_to_send,
                enqueued.len(),
                "All enqueued messages were not successful"
            );

            let streamed_res = queue
                .clone()
                .stream_messages()
                .take(msgs_to_send as u64)
                .collect();

            let streamed_res = rt.block_on(streamed_res);
            assert!(streamed_res.is_ok());
        });
    }

}
