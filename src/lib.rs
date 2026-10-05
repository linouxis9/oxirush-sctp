#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

mod connected_socket;
mod endpoint;
mod listener;
mod options;
mod socket;

#[doc(inline)]
pub use socket::Socket;

#[doc(inline)]
pub use listener::Listener;

#[doc(inline)]
pub use connected_socket::ConnectedSocket;

#[doc(inline)]
pub use endpoint::OneToManyEndpoint;

#[doc(inline)]
pub use options::SocketOptions;

mod internal;

mod consts;

mod types;

#[doc(inline)]
pub use types::{
    AssocChangeState, AssociationChange, AssociationId, BindxFlags, ConnState, ConnStatus, Event,
    EventSubscriptionError, Notification, NotificationOrData, NxtInfo, PeerAddress,
    PeerAddressChange, PeerAddressParams, PeerAddressState, RcvInfo, ReceivedData, RtoInfo,
    SendData, SendFailure, SendInfo, SendOptions, Shutdown, SocketToAssociation,
    SubscribeEventAssocId,
};
