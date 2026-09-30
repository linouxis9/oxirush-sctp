//! Synchronous Linux operations. Higher layers use only borrowed live descriptors.
mod addresses;
mod descriptor;
mod message;
mod options;

pub(crate) use addresses::*;
pub(crate) use descriptor::*;
pub(crate) use message::*;
pub(crate) use options::*;

const SOL_SCTP: libc::c_int = libc::IPPROTO_SCTP;
