use crate::Incoming;

use std::{collections::VecDeque, result::Result, time::Instant};
use mqtt4bytes::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MqttConnectionStatus {
    Handshake,
    Connected,
    Disconnecting,
    Disconnected,
}

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// Broker's error reply to client's connect packet
    #[error("Connect return code `{0:?}`")]
    Connect(ConnectReturnCode),
    /// Invalid state for a given operation
    #[error("Invalid state for a given operation")]
    InvalidState,
    /// Received a packet (ack) which isn't asked for
    #[error("Received a packet (ack) which isn't asked for")]
    Unsolicited,
    /// Last pingreq isn't acked
    #[error("Last pingreq isn't acked")]
    AwaitPingResp,
    /// Received a wrong packet while waiting for another packet
    #[error("Received a wrong packet while waiting for another packet")]
    WrongPacket,
}

/// State of the mqtt connection.
// Methods will just modify the state of the object without doing any network operations
// This abstracts the functionality better so that it's easy to switch between synchronous code,
// tokio (or) async/await
#[derive(Debug, Clone)]
pub struct MqttState {
    /// Connection status
    pub connection_status: MqttConnectionStatus,
    /// Status of last ping
    pub await_pingresp: bool,
    /// Last incoming packet time
    pub last_incoming: Instant,
    /// Last outgoing packet time
    pub last_outgoing: Instant,
    /// Packet id of the last outgoing packet
    pub last_pkid: u16,
    /// Outgoing QoS 1, 2 publishes which aren't acked yet
    pub outgoing_pub: VecDeque<Publish>,
    /// Packet ids of released QoS 2 publishes
    pub outgoing_rel: VecDeque<u16>,
    /// Packet ids on incoming QoS 2 publishes
    pub incoming_pub: VecDeque<u16>,
}

impl MqttState {
    /// Creates new mqtt state. Same state should be used during a
    /// connection for persistent sessions while new state should
    /// instantiated for clean sessions
    pub fn new() -> Self {
        MqttState {
            connection_status: MqttConnectionStatus::Disconnected,
            await_pingresp: false,
            last_incoming: Instant::now(),
            last_outgoing: Instant::now(),
            last_pkid: 0,
            outgoing_pub: VecDeque::new(),
            outgoing_rel: VecDeque::new(),
            incoming_pub: VecDeque::new(),
        }
    }

    /// Consolidates handling of all outgoing mqtt packet logic. Returns a packet which should
    /// be put on to the network by the eventloop
    pub(crate) fn handle_outgoing_packet(
        &mut self,
        packet: Packet,
    ) -> Result<(Option<Incoming>, Option<Packet>), StateError> {
        let out = match packet {
            Packet::Publish(publish) => self.handle_outgoing_publish(publish)?,
            Packet::Subscribe(subscribe) => self.handle_outgoing_subscribe(subscribe)?,
            Packet::PingReq => self.handle_outgoing_ping()?,
            _ => unimplemented!(),
        };

        self.last_outgoing = Instant::now();
        let request = Some(out);
        let notification = None;
        Ok((notification, request))
    }

    /// Consolidates handling of all incoming mqtt packets. Returns a `Notification` which for the
    /// user to consume and `Packet` which for the eventloop to put on the network
    /// E.g For incoming QoS1 publish packet, this method returns (Publish, Puback). Publish packet will
    /// be forwarded to user and Pubck packet will be written to network
    pub(crate) fn handle_incoming_packet(
        &mut self,
        packet: Packet,
    ) -> Result<(Option<Incoming>, Option<Packet>), StateError> {
        let out = match packet {
            Packet::PingResp => self.handle_incoming_pingresp(),
            Packet::Publish(publish) => self.handle_incoming_publish(publish.clone()),
            Packet::SubAck(suback) => self.handle_incoming_suback(suback),
            Packet::UnsubAck(unsuback) => self.handle_incoming_unsuback(unsuback),
            Packet::PubAck(puback) => self.handle_incoming_puback(puback),
            Packet::PubRec(pubrec) => self.handle_incoming_pubrec(pubrec),
            Packet::PubRel(pubrel) => self.handle_incoming_pubrel(pubrel),
            Packet::PubComp(pubcomp) => self.handle_incoming_pubcomp(pubcomp),
            _ => {
                error!("Invalid incoming paket = {:?}", packet);
                Ok((None, None))
            }
        };

        self.last_incoming = Instant::now();
        out
    }

    /// Adds next packet identifier to QoS 1 and 2 publish packets and returns
    /// it buy wrapping publish in packet
    fn handle_outgoing_publish(&mut self, publish: Publish) -> Result<Packet, StateError> {
        let publish = match publish.qos {
            QoS::AtMostOnce => publish,
            QoS::AtLeastOnce | QoS::ExactlyOnce => self.add_packet_id_and_save(publish),
        };

        debug!(
            "Publish. Topic = {:?}, Pkid = {:?}, Payload Size = {:?}",
            publish.topic,
            publish.pkid,
            publish.payload.len()
        );

        Ok(Packet::Publish(publish))
    }

    /// Iterates through the list of stored publishes and removes the publish with the
    /// matching packet identifier. Removal is now a O(n) operation. This should be
    /// usually ok in case of acks due to ack ordering in normal conditions. But in cases
    /// where the broker doesn't guarantee the order of acks, the performance won't be optimal
    fn handle_incoming_puback(&mut self, puback: PubAck) -> Result<(Option<Incoming>, Option<Packet>), StateError> {
        match self.outgoing_pub.iter().position(|x| x.pkid == puback.pkid) {
            Some(index) => {
                let _publish = self.outgoing_pub.remove(index).expect("Wrong index");

                let request = None;
                let incoming = Some(Incoming::Puback(puback));
                Ok((incoming, request))
            }
            None => {
                error!("Unsolicited puback packet: {:?}", puback.pkid);
                Err(StateError::Unsolicited)
            }
        }
    }

    fn handle_incoming_suback(
        &mut self,
        suback: SubAck,
    ) -> Result<(Option<Incoming>, Option<Packet>), StateError> {
        let incoming = Some(Incoming::Suback(suback));
        let response = None;
        Ok((incoming, response))
    }

    fn handle_incoming_unsuback(
        &mut self,
        unsuback: UnsubAck,
    ) -> Result<(Option<Incoming>, Option<Packet>), StateError> {
        let incoming = Some(Incoming::Unsuback(unsuback));
        let response = None;
        Ok((incoming, response))
    }

    /// Iterates through the list of stored publishes and removes the publish with the
    /// matching packet identifier. Removal is now a O(n) operation. This should be
    /// usually ok in case of acks due to ack ordering in normal conditions. But in cases
    /// where the broker doesn't guarantee the order of acks, the performance won't be optimal
    fn handle_incoming_pubrec(
        &mut self,
        pubrec: PubRec,
    ) -> Result<(Option<Incoming>, Option<Packet>), StateError> {
        match self.outgoing_pub.iter().position(|x| x.pkid == pubrec.pkid) {
            Some(index) => {
                let _ = self.outgoing_pub.remove(index);
                self.outgoing_rel.push_back(pubrec.pkid);

                let response = Some(Packet::PubRel(PubRel::new(pubrec.pkid)));
                let incoming = Some(Incoming::Pubrec(pubrec));
                Ok((incoming, response))
            }
            None => {
                error!("Unsolicited pubrec packet: {:?}", pubrec.pkid);
                Err(StateError::Unsolicited)
            }
        }
    }

    /// Results in a publish notification in all the QoS cases. Replys with an ack
    /// in case of QoS1 and Replys rec in case of QoS while also storing the message
    fn handle_incoming_publish(
        &mut self,
        publish: Publish,
    ) -> Result<(Option<Incoming>, Option<Packet>), StateError> {
        let qos = publish.qos;

        match qos {
            QoS::AtMostOnce => {
                let incoming = Incoming::Publish(publish);
                Ok((Some(incoming), None))
            }
            QoS::AtLeastOnce => {
                let pkid = publish.pkid;
                let response = Packet::PubAck(PubAck::new(pkid));
                let incoming = Incoming::Publish(publish);
                Ok((Some(incoming), Some(response)))
            }
            QoS::ExactlyOnce => {
                let pkid = publish.pkid;
                let response = Packet::PubRec(PubRec::new(pkid));
                let incoming = Incoming::Publish(publish);

                self.incoming_pub.push_back(pkid);
                Ok((Some(incoming), Some(response)))
            }
        }
    }

    fn handle_incoming_pubrel(
        &mut self,
        pubrel: PubRel,
    ) -> Result<(Option<Incoming>, Option<Packet>), StateError> {
        match self.incoming_pub.iter().position(|x| *x == pubrel.pkid) {
            Some(index) => {
                let _ = self.incoming_pub.remove(index);
                let response = Packet::PubComp(PubComp::new(pubrel.pkid));
                Ok((None, Some(response)))
            }
            None => {
                error!("Unsolicited pubrel packet: {:?}", pubrel.pkid);
                Err(StateError::Unsolicited)
            }
        }
    }

    fn handle_incoming_pubcomp(
        &mut self,
        pubcomp: PubComp,
    ) -> Result<(Option<Incoming>, Option<Packet>), StateError> {
        match self.outgoing_rel.iter().position(|x| *x == pubcomp.pkid) {
            Some(index) => {
                self.outgoing_rel.remove(index).expect("Wrong index");
                let incoming = Some(Incoming::Pubcomp(pubcomp));
                let response = None;
                Ok((incoming, response))
            }
            _ => {
                error!("Unsolicited pubcomp packet: {:?}", pubcomp.pkid);
                Err(StateError::Unsolicited)
            }
        }
    }

    /// check when the last control packet/pingreq packet is received and return
    /// the status which tells if keep alive time has exceeded
    /// NOTE: status will be checked for zero keepalive times also
    fn handle_outgoing_ping(&mut self) -> Result<Packet, StateError> {
        let elapsed_in = self.last_incoming.elapsed();
        let elapsed_out = self.last_outgoing.elapsed();

        // raise error if last ping didn't receive ack
        if self.await_pingresp {
            error!("Error awaiting for last ping response");
            return Err(StateError::AwaitPingResp);
        }

        self.await_pingresp = true;

        debug!(
            "Pingreq,
            last incoming packet before {} millisecs,
            last outgoing request before {} millisecs",
            elapsed_in.as_millis(),
            elapsed_out.as_millis()
        );

        Ok(Packet::PingReq)
    }

    fn handle_incoming_pingresp(
        &mut self,
    ) -> Result<(Option<Incoming>, Option<Packet>), StateError> {
        self.await_pingresp = false;
        let incoming = Some(Incoming::PingResp);
        Ok((incoming, None))
    }

    fn handle_outgoing_subscribe(
        &mut self,
        mut subscription: Subscribe,
    ) -> Result<Packet, StateError> {
        let pkid = self.next_pkid();
        subscription.pkid = pkid;

        debug!(
            "Subscribe. Topics = {:?}, Pkid = {:?}",
            subscription.topics, subscription.pkid
        );
        Ok(Packet::Subscribe(subscription))
    }

    pub fn handle_outgoing_connect(&mut self) -> Result<(), StateError> {
        self.connection_status = MqttConnectionStatus::Handshake;
        Ok(())
    }

    pub fn handle_incoming_connack(&mut self, packet: Packet) -> Result<(), StateError> {
        let connack = match packet {
            Packet::ConnAck(connack) => connack,
            packet => {
                error!("Invalid packet. Expecting connack. Received = {:?}", packet);
                self.connection_status = MqttConnectionStatus::Disconnected;
                return Err(StateError::WrongPacket);
            }
        };

        match connack.code {
            ConnectReturnCode::Accepted
                if self.connection_status == MqttConnectionStatus::Handshake =>
            {
                self.connection_status = MqttConnectionStatus::Connected;
                Ok(())
            }
            ConnectReturnCode::Accepted
                if self.connection_status != MqttConnectionStatus::Handshake =>
            {
                error!(
                    "Invalid state. Expected = {:?}, Current = {:?}",
                    MqttConnectionStatus::Handshake,
                    self.connection_status
                );
                self.connection_status = MqttConnectionStatus::Disconnected;
                Err(StateError::InvalidState)
            }
            code => {
                error!("Connection failed. Connection error = {:?}", code);
                self.connection_status = MqttConnectionStatus::Disconnected;
                Err(StateError::Connect(code))
            }
        }
    }

    /// Add publish packet to the state and return the packet. This method clones the
    /// publish packet to save it to the state.
    /// TODO Measure Arc vs copy perf and take a call regarding clones
    fn add_packet_id_and_save(&mut self, mut publish: Publish) -> Publish {
        let publish = match publish.pkid {
            // consider PacketIdentifier(0) and None as uninitialized packets
            0 => {
                let pkid = self.next_pkid();
                publish.set_pkid(pkid);
                publish
            }
            _ => publish,
        };

        self.outgoing_pub.push_back(publish.clone());
        publish
    }

    /// Increment the packet identifier from the state and roll it when it reaches its max
    /// http://stackoverflow.com/questions/11115364/mqtt-messageid-practical-implementation
    fn next_pkid(&mut self) -> u16 {
        let mut pkid = self.last_pkid;
        if pkid == 65_535 {
            pkid = 0;
        }
        self.last_pkid = pkid + 1;
        self.last_pkid
    }
}

#[cfg(test)]
mod test {
    use super::{MqttConnectionStatus, MqttState, Packet, StateError};
    use crate::{Incoming, MqttOptions};
    use mqtt4bytes::*;

    fn build_outgoing_publish(qos: QoS) -> Publish {
        let topic = "hello/world".to_owned();
        let payload = vec![1, 2, 3];

        let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload);
        publish.qos = qos;
        publish
    }

    fn build_incoming_publish(qos: QoS, pkid: u16) -> Publish {
        let topic = "hello/world".to_owned();
        let payload = vec![1, 2, 3];

        let mut publish = Publish::new(topic, QoS::AtLeastOnce, payload);
        publish.pkid = pkid;
        publish.qos = qos;
        publish
    }

    fn build_mqttstate() -> MqttState {
        MqttState::new()
    }

    #[test]
    fn next_pkid_roll() {
        let mut mqtt = build_mqttstate();
        let mut pkt_id = 0;

        for _ in 0..65536 {
            pkt_id = mqtt.next_pkid();
        }
        assert_eq!(1, pkt_id);
    }

    #[test]
    fn outgoing_publish_handle_should_set_pkid_correctly_and_add_publish_to_queue_correctly() {
        let mut mqtt = build_mqttstate();

        // QoS0 Publish
        let publish = build_outgoing_publish(QoS::AtMostOnce);

        // Packet id shouldn't be set and publish shouldn't be saved in queue
        let publish_out = match mqtt.handle_outgoing_publish(publish) {
            Ok(Packet::Publish(p)) => p,
            _ => panic!("Invalid packet. Should've been a publish packet"),
        };
        assert_eq!(publish_out.pkid, 0);
        assert_eq!(mqtt.outgoing_pub.len(), 0);

        // QoS1 Publish
        let publish = build_outgoing_publish(QoS::AtLeastOnce);

        // Packet id should be set and publish should be saved in queue
        let publish_out = match mqtt.handle_outgoing_publish(publish.clone()) {
            Ok(Packet::Publish(p)) => p,
            _ => panic!("Invalid packet. Should've been a publish packet"),
        };
        assert_eq!(publish_out.pkid, 1);
        assert_eq!(mqtt.outgoing_pub.len(), 1);

        // Packet id should be incremented and publish should be saved in queue
        let publish_out = match mqtt.handle_outgoing_publish(publish.clone()) {
            Ok(Packet::Publish(p)) => p,
            _ => panic!("Invalid packet. Should've been a publish packet"),
        };
        assert_eq!(publish_out.pkid, 2);
        assert_eq!(mqtt.outgoing_pub.len(), 2);

        // QoS1 Publish
        let publish = build_outgoing_publish(QoS::ExactlyOnce);

        // Packet id should be set and publish should be saved in queue
        let publish_out = match mqtt.handle_outgoing_publish(publish.clone()) {
            Ok(Packet::Publish(p)) => p,
            _ => panic!("Invalid packet. Should've been a publish packet"),
        };
        assert_eq!(publish_out.pkid, 3);
        assert_eq!(mqtt.outgoing_pub.len(), 3);

        // Packet id should be incremented and publish should be saved in queue
        let publish_out = match mqtt.handle_outgoing_publish(publish.clone()) {
            Ok(Packet::Publish(p)) => p,
            _ => panic!("Invalid packet. Should've been a publish packet"),
        };
        assert_eq!(publish_out.pkid, 4);
        assert_eq!(mqtt.outgoing_pub.len(), 4);
    }

    #[test]
    fn incoming_publish_should_be_added_to_queue_correctly() {
        let mut mqtt = build_mqttstate();

        // QoS0, 1, 2 Publishes
        let publish1 = build_incoming_publish(QoS::AtMostOnce, 1);
        let publish2 = build_incoming_publish(QoS::AtLeastOnce, 2);
        let publish3 = build_incoming_publish(QoS::ExactlyOnce, 3);

        mqtt.handle_incoming_publish(publish1).unwrap();
        mqtt.handle_incoming_publish(publish2).unwrap();
        mqtt.handle_incoming_publish(publish3).unwrap();

        let pkid = *mqtt.incoming_pub.get(0).unwrap();

        // only qos2 publish should be add to queue
        assert_eq!(mqtt.incoming_pub.len(), 1);
        assert_eq!(pkid, 3);
    }

    #[test]
    fn incoming_qos2_publish_should_send_rec_to_network_and_publish_to_user() {
        let mut mqtt = build_mqttstate();
        let publish = build_incoming_publish(QoS::ExactlyOnce, 1);

        let (notification, request) = mqtt.handle_incoming_publish(publish).unwrap();

        match notification {
            Some(Incoming::Publish(publish)) => {
                assert_eq!(publish.pkid, 1)
            }
            _ => panic!("Invalid notification: {:?}", notification),
        }

        match request {
            Some(Packet::PubRec(pubrec)) => assert_eq!(pubrec.pkid, 1),
            _ => panic!("Invalid network request: {:?}", request),
        }
    }

    #[test]
    fn incoming_puback_should_remove_correct_publish_from_queue() {
        let mut mqtt = build_mqttstate();

        let publish1 = build_outgoing_publish(QoS::AtLeastOnce);
        let publish2 = build_outgoing_publish(QoS::ExactlyOnce);

        mqtt.handle_outgoing_publish(publish1).unwrap();
        mqtt.handle_outgoing_publish(publish2).unwrap();

        mqtt.handle_incoming_puback(PubAck::new(1)).unwrap();
        assert_eq!(mqtt.outgoing_pub.len(), 1);

        let backup = mqtt.outgoing_pub.get(0).clone();
        assert_eq!(backup.unwrap().pkid, 2);

        mqtt.handle_incoming_puback(PubAck::new(2)).unwrap();
        assert_eq!(mqtt.outgoing_pub.len(), 0);
    }

    #[test]
    fn incoming_pubrec_should_release_correct_publish_from_queue_and_add_releaseid_to_rel_queue() {
        let mut mqtt = build_mqttstate();

        let publish1 = build_outgoing_publish(QoS::AtLeastOnce);
        let publish2 = build_outgoing_publish(QoS::ExactlyOnce);

        let _publish_out = mqtt.handle_outgoing_publish(publish1);
        let _publish_out = mqtt.handle_outgoing_publish(publish2);

        mqtt.handle_incoming_pubrec(PubRec::new(2)).unwrap();
        assert_eq!(mqtt.outgoing_pub.len(), 1);

        // check if the remaining element's pkid is 1
        let backup = mqtt.outgoing_pub.get(0).clone();
        assert_eq!(backup.unwrap().pkid, 1);

        assert_eq!(mqtt.outgoing_rel.len(), 1);

        // check if the  element's pkid is 2
        let pkid = *mqtt.outgoing_rel.get(0).unwrap();
        assert_eq!(pkid, 2);
    }

    #[test]
    fn incoming_pubrec_should_send_release_to_network_and_nothing_to_user() {
        let mut mqtt = build_mqttstate();

        let publish = build_outgoing_publish(QoS::ExactlyOnce);
        mqtt.handle_outgoing_publish(publish).unwrap();

        let (notification, request) = mqtt.handle_incoming_pubrec(PubRec::new(1)).unwrap();

        match notification {
            Some(Incoming::Pubrec(pubrec)) => assert_eq!(pubrec.pkid, 1),
            _ => panic!("Invalid notification"),
        }

        match request {
            Some(Packet::PubRel(pubrel)) => assert_eq!(pubrel.pkid, 1),
            _ => panic!("Invalid network request: {:?}", request),
        }
    }

    #[test]
    fn incoming_pubrel_should_send_comp_to_network_and_nothing_to_user() {
        let mut mqtt = build_mqttstate();
        let publish = build_incoming_publish(QoS::ExactlyOnce, 1);

        mqtt.handle_incoming_publish(publish).unwrap();
        println!("{:?}", mqtt);
        let (notification, request) = mqtt.handle_incoming_pubrel(PubRel::new(1)).unwrap();

        match notification {
            None => assert!(true),
            _ => panic!("Invalid notification: {:?}", notification),
        }

        match request {
            Some(Packet::PubComp(pubcomp)) => assert_eq!(pubcomp.pkid, 1),
            _ => panic!("Invalid network request: {:?}", request),
        }
    }

    #[test]
    fn incoming_pubcomp_should_release_correct_pkid_from_release_queue() {
        let mut mqtt = build_mqttstate();
        let publish = build_outgoing_publish(QoS::ExactlyOnce);

        mqtt.handle_outgoing_publish(publish).unwrap();
        mqtt.handle_incoming_pubrec(PubRec::new(1)).unwrap();
        println!("{:?}", mqtt);

        mqtt.handle_incoming_pubcomp(PubComp::new(1)).unwrap();
        assert_eq!(mqtt.outgoing_pub.len(), 0);
    }

    #[test]
    fn outgoing_ping_handle_should_throw_errors_for_no_pingresp() {
        let mut mqtt = build_mqttstate();
        let mut opts = MqttOptions::new("test", "localhost", 1883);
        opts.set_keep_alive(10);
        mqtt.connection_status = MqttConnectionStatus::Connected;
        mqtt.handle_outgoing_ping().unwrap();

        // network activity other than pingresp
        let publish = build_outgoing_publish(QoS::AtLeastOnce);
        mqtt.handle_outgoing_packet(Packet::Publish(publish))
            .unwrap();
        mqtt.handle_incoming_packet(Packet::PubAck(PubAck::new(1)))
            .unwrap();

        // should throw error because we didn't get pingresp for previous ping
        match mqtt.handle_outgoing_ping() {
            Ok(_) => panic!("Should throw pingresp await error"),
            Err(StateError::AwaitPingResp) => (),
            Err(e) => panic!("Should throw pingresp await error. Error = {:?}", e),
        }
    }

    #[test]
    fn outgoing_ping_handle_should_succeed_if_pingresp_is_received() {
        let mut mqtt = build_mqttstate();

        let mut opts = MqttOptions::new("test", "localhost", 1883);
        opts.set_keep_alive(10);

        mqtt.connection_status = MqttConnectionStatus::Connected;

        // should ping
        mqtt.handle_outgoing_ping().unwrap();
        mqtt.handle_incoming_packet(Packet::PingResp).unwrap();

        // should ping
        mqtt.handle_outgoing_ping().unwrap();
    }
}