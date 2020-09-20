use thiserror::Error;

use crate::connection_manager::{Command, CommandQueue, Message};
use crate::{EventLoop, QoS};

#[derive(Clone)]
pub struct AsyncClient {
    cmd: CommandQueue,
}

#[derive(Error, Debug)]
pub enum CommandError {
    #[error("manager was disconnected")]
    ManagerDisconnect,
}

impl AsyncClient {
    pub fn new(cmd: CommandQueue) -> AsyncClient {
        AsyncClient { cmd }
    }

    pub async fn publish(
        &mut self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
    ) -> Result<(), CommandError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let msg = crate::Publish::new(topic, qos, payload);
        self.cmd
            .send(Command::Publish((msg, tx)))
            .await
            .map_err(|_| CommandError::ManagerDisconnect)?;
        let _r = rx.await.map_err(|_| CommandError::ManagerDisconnect)?;
        Ok(())
    }

    pub async fn subscribe(&mut self, topic: &str, qos: QoS) -> Result<(), CommandError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let msg = crate::Subscribe::new(topic, qos);
        self.cmd
            .send(Command::Subscribe((msg, tx)))
            .await
            .map_err(|_| CommandError::ManagerDisconnect)?;
        let _r = rx.await.map_err(|_| CommandError::ManagerDisconnect)?;
        Ok(())
    }

    pub async fn unsubscribe(&mut self, topic: &str) -> Result<(), CommandError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let msg = crate::Unsubscribe::new(topic);
        self.cmd
            .send(Command::Unsubscribe((msg, tx)))
            .await
            .map_err(|_| CommandError::ManagerDisconnect)?;
        let _r = rx.await.map_err(|_| CommandError::ManagerDisconnect)?;
        Ok(())
    }

    pub async fn disconnect(&mut self) -> Result<(), CommandError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.cmd
            .send(Command::Disconnect(tx))
            .await
            .map_err(|_| CommandError::ManagerDisconnect)?;
        let _r = rx.await.map_err(|_| CommandError::ManagerDisconnect)?;
        Ok(())
    }

    pub async fn route(
        &mut self,
        topic: &str,
        buffer: usize,
    ) -> Result<tokio::sync::mpsc::Receiver<Message>, CommandError> {
        let (tx, rx) = tokio::sync::mpsc::channel(buffer);
        self.cmd
            .send(Command::Route((topic.to_string(), tx)))
            .await
            .map_err(|_| CommandError::ManagerDisconnect)?;
        Ok(rx)
    }
}

#[cfg(test)]
mod test {
    use crate::connection_manager::ConnectionManager;
    use crate::{EventLoop, MqttOptions, QoS};

    use super::AsyncClient;

    #[tokio::main]
    async fn pubsub() {
        let mqttoptions = MqttOptions::new("test-1", "localhost", 1883);
        let eventloop = EventLoop::new(mqttoptions, 10);
        let mut manager = ConnectionManager::new(eventloop, 10);
        let cmd = manager.command_queue();

        // Drive the eventloop in another task
        tokio::spawn(async move {
            loop {
                let msg = manager.next().await.unwrap();
            }
        });

        let mut client = AsyncClient::new(cmd);
        client.subscribe("topic/#", QoS::AtMostOnce).await.unwrap();

        // It should be possible to route to arbitrary filters/topics. The manager
        // is responsible to route all received messages to all queues and should
        // start blocking if any of the queues blocks.
        let mut rx = client.route("topic/#", 2).await.unwrap();

        let c = client.clone();
        tokio::spawn(async move {
            publisher_task("A", c).await;
        });

        let c = client.clone();
        tokio::spawn(async move {
            publisher_task("B", c).await;
        });

        // Give the publishers some time to fill up the queue to demonstrate backpressure
        tokio::time::delay_for(std::time::Duration::from_millis(5000)).await;

        // Now start handling incoming messages
        println!("Start handling incoming messages");
        loop {
            tokio::select! {
                Some(msg) = rx.recv() => println!("Handler: topic = {}; payload = {:?}", msg.topic, msg.payload),
                _ = tokio::time::delay_for(std::time::Duration::from_millis(2000)) => {
                    println!("No new messages for two seconds - shutting down");
                    break;
                }
            }
        }

        client.disconnect().await.unwrap();
    }

    async fn publisher_task(name: &str, mut client: AsyncClient) {
        println!("Start publisher task: {}", name);
        let topic = format!("topic/{}", name);
        for i in 0..10 {
            match client
                .publish(&topic, &format!("test: {}", i).as_bytes(), QoS::AtLeastOnce)
                .await
            {
                Ok(_) => println!("Publisher {}: Successfully sent test number {}", name, i),
                Err(_) => println!("Publisher {}: Failed to send test number {}", name, i),
            }
            tokio::time::delay_for(std::time::Duration::from_millis(1000)).await
        }
    }

    #[test]
    fn test_pubsub() {
        pubsub()
    }
}
