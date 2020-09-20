use tokio::sync::{mpsc, oneshot};

use crate::{
    ConnectionError, Event, EventLoop, Incoming, Outgoing, Publish, QoS, Request, Subscribe,
    Unsubscribe,
};

#[derive(Clone)]
pub struct Message {
    pub topic: String,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
pub enum Command {
    Publish((Publish, oneshot::Sender<()>)),
    Subscribe((Subscribe, oneshot::Sender<()>)),
    Unsubscribe((Unsubscribe, oneshot::Sender<()>)),
    Disconnect(oneshot::Sender<()>),
    Route((String, mpsc::Sender<Message>)),
}

pub type CommandQueue = mpsc::Sender<Command>;

pub struct ConnectionManager {
    eventloop: EventLoop,
    cmd_tx: mpsc::Sender<Command>,
    cmd_rx: mpsc::Receiver<Command>,
    routes: Vec<(String, mpsc::Sender<Message>)>,
}

impl ConnectionManager {
    pub fn new(eventloop: EventLoop, buffer: usize) -> ConnectionManager {
        let (tx, rx) = mpsc::channel(buffer);
        ConnectionManager {
            eventloop,
            cmd_tx: tx,
            cmd_rx: rx,
            routes: vec![],
        }
    }

    pub fn command_queue(&self) -> mpsc::Sender<Command> {
        self.cmd_tx.clone()
    }

    pub async fn next(&mut self) -> Result<(), ConnectionError> {
        loop {
            tokio::select! {
                Ok(event) = self.eventloop.poll() => {
                    match event {
                        Event::Incoming(Incoming::Publish(p)) => {
                            for (filter, tx) in self.routes.iter_mut() {
                                if mqtt4bytes::matches(&p.topic, filter) {
                                    tx.send(Message {topic: p.topic.clone(), payload: vec![]}).await;
                                }
                            }
                        }
                        _ => {}
                    }
                    return Ok(());
                }
                Some(cmd) = self.cmd_rx.recv() => {
                    self.handle_command(cmd).await;
                }
            }
        }
    }

    async fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::Publish((msg, cb)) => {
                self.eventloop
                    .handle()
                    .send(Request::Publish(msg))
                    .await
                    .unwrap();
                cb.send(());
            }
            Command::Subscribe((msg, cb)) => {
                self.eventloop
                    .handle()
                    .send(Request::Subscribe(msg))
                    .await
                    .unwrap();
                cb.send(());
            }
            Command::Unsubscribe((msg, cb)) => {
                self.eventloop
                    .handle()
                    .send(Request::Unsubscribe(msg))
                    .await
                    .unwrap();
                cb.send(());
            }
            Command::Disconnect(cb) => {
                self.eventloop
                    .handle()
                    .send(Request::Disconnect)
                    .await
                    .unwrap();
                cb.send(());
            }
            Command::Route((filter, tx)) => {
                self.routes.push((filter, tx));
            }
        };
    }
}
