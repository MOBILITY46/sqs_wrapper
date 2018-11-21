extern crate futures;
extern crate log;
extern crate rusoto_core;
extern crate rusoto_sqs;

use log::debug;

use futures::{Async, Future, Poll, Stream};
use rusoto_core::{Region, RusotoFuture};
use rusoto_sqs::{self as sqs, Sqs};

pub struct RecordStream {
    client: Client,
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
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(err) => Err(err),
            Ok(Async::Ready(res)) => {
                // Request more
                match res.messages {
                    Some(messages) => {
                        debug!("Got {} messages", messages.len());
                        messages
                            .into_iter()
                            .for_each(|msg| self.message_buffer.push(msg));
                    }

                    None => {
                        debug!("Got 0 messages");
                    }
                }

                self.future = self.client.receive_messages();
                self.poll()
            }
        }
    }
}

pub struct Client {
    queue_url: String,
    wait_time: i64,
    visibility_timeout: i64,
    region: Region,
    c: sqs::SqsClient,
}

/// Try to convert the example in wait_and_read_one to use futures and Tokio.
impl Client {
    pub fn new<S: Into<String>>(region: Region, queue_url: S) -> Client {
        Client {
            c: sqs::SqsClient::new(region.clone()),
            region: region.clone(),
            queue_url: queue_url.into(),
            wait_time: 3,
            visibility_timeout: 10,
        }
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
            visibility_timeout: Some(self.visibility_timeout),
            wait_time_seconds: Some(self.wait_time),
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
            client: Client {
                queue_url: self.queue_url.clone(),
                region: self.region.clone(),
                wait_time: self.wait_time,
                visibility_timeout: self.visibility_timeout,
                c: sqs::SqsClient::new(self.region.clone()),
            },
            future: initial_future,
            message_buffer: Vec::with_capacity(10),
        }
    }
}

#[cfg(test)]
mod tests {

    extern crate dotenv;
    extern crate tokio;

    use self::tokio::runtime::Runtime;
    use std::time::SystemTime;
    use std::{env, sync::Arc};

    use super::*;

    fn setup_test() -> (Client, Runtime) {
        assert!(dotenv::dotenv().is_ok(), "Dotenv failed to initialize");
        let url = env::var("SQS_WRAPPER_TEST_URL");
        assert!(url.is_ok(), "Env var SQS_WRAPPER_TEST_URL must be set");

        let c = Client::new(rusoto_core::Region::EuCentral1, url.unwrap());
        let rt = Runtime::new();
        assert!(rt.is_ok(), "Runtime failed to initialize");

        (c, rt.unwrap())
    }

    #[test]
    fn test_enqueue_and_delete_message() {
        let (mut c, mut rt) = setup_test();
        c.wait_time = 1;

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();

        let payload = format!(
            "{{ \"payload\": \"My cool payload used for testing\", \"timestamp\": \"{}.{}\"}}",
            now.as_secs(),
            now.subsec_millis(),
        );

        // ENQUEUE
        let enq_res = rt.block_on(c.enqueue_message(payload.clone()));
        assert!(enq_res.is_ok(), "Error enqueuing message");

        let enq_res = enq_res.unwrap();
        assert!(enq_res.message_id.is_some(), "Message ID was None");

        let message_id = enq_res.message_id.unwrap();
        assert!(0 < message_id.len(), "Message ID was empty string");

        // DEQUEUE
        let deq_res = rt.block_on(c.receive_messages());
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

        let delete_res = rt.block_on(c.delete_message(receipt_handle));

        assert!(delete_res.is_ok(), "Error deleting the message");
    }

}