#![allow(dead_code)]
//! Constants used by lower level `libc` APIs
//!
//! These constants come from `linux/sctp.h`

// Constants used by `sctp_bindx`
pub(crate) static SCTP_SOCKOPT_BINDX_ADD: libc::c_int = 100;
pub(crate) static SCTP_SOCKOPT_BINDX_REM: libc::c_int = 101;

// peel off a one to many socket
pub(crate) static SCTP_SOCKOPT_PEELOFF: libc::c_int = 102;
pub(crate) static SCTP_SOCKOPT_PEELOFF_FLAGS: libc::c_int = 122;

// get peer/localaddrs
pub(crate) static SCTP_GET_PEER_ADDRS: libc::c_int = 108;
pub(crate) static SCTP_GET_LOCAL_ADDRS: libc::c_int = 109;

// To connect to an SCTP server.
pub(crate) static SCTP_SOCKOPT_CONNECTX: libc::c_int = 110;
pub(crate) static SCTP_SOCKOPT_CONNECTX3: libc::c_int = 111;

// To subscribe to SCTP Events
pub(crate) static SCTP_EVENT: libc::c_int = 127;

//
pub(crate) static MSG_NOTIFICATION: u32 = 0x8000;

// Notification Types Constants
pub(crate) const SCTP_ASSOC_CHANGE: u16 = (1 << 15) + 0x0001;
pub(crate) const SCTP_SHUTDOWN: u16 = (1 << 15) + 0x0005;

// Retransmission timeout parameters
pub(crate) const SCTP_RTOINFO: libc::c_int = 0;

// Init Message used for `setsockopt`
pub(crate) const SCTP_INITMSG: libc::c_int = 2;

// Disable Nagle-like bundling delays
pub(crate) const SCTP_NODELAY: libc::c_int = 3;

// Receving RCVINFO and NXTINFO
pub(crate) const SCTP_RECVRCVINFO: libc::c_int = 32;
pub(crate) const SCTP_RECVNXTINFO: libc::c_int = 33;
pub(crate) const SCTP_DEFAULT_SNDINFO: libc::c_int = 34;

// Get SCTP Status
pub(crate) const SCTP_STATUS: libc::c_int = 14;

// The state, window, round trip and timeout of one path
pub(crate) const SCTP_GET_PEER_ADDR_INFO: libc::c_int = 15;

// Whether the parts of the messages of several associations may come in turns
pub(crate) const SCTP_FRAGMENT_INTERLEAVE: libc::c_int = 18;

// Heartbeats and path retransmission limits.
pub(crate) const SCTP_PEER_ADDR_PARAMS: libc::c_int = 9;

// Association parameters, the primary path, the delay of acknowledgements
pub(crate) const SCTP_ASSOCINFO: libc::c_int = 1;
pub(crate) const SCTP_PRIMARY_ADDR: libc::c_int = 6;
pub(crate) const SCTP_DELAYED_SACK: libc::c_int = 16;

// Flag of a send that aborts the association, with the message as the reason
pub(crate) const SCTP_ABORT: u16 = 1 << 2;
// Flag of a send without a message that shuts the association down (`MSG_FIN`)
pub(crate) const SCTP_EOF: u16 = 0x200;
// Flag of a send to every association of a one-to-many socket
pub(crate) const SCTP_SENDALL: u16 = 1 << 6;

// The values of `SO_MEMINFO`, and the two of them that a batch of sends reads
pub(crate) const SK_MEMINFO_VARS: usize = 9;
pub(crate) const SK_MEMINFO_SNDBUF: usize = 3;
pub(crate) const SK_MEMINFO_WMEM_QUEUED: usize = 5;
