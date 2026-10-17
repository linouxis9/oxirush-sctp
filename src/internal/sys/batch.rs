//! Messages sent and received several at a time: `sendmmsg` and `recvmmsg`.
use super::message::ancillary_info;
use crate::SendInfo;
use crate::consts::MSG_NOTIFICATION;
use crate::internal::receive::Piece;
use crate::types::CmsgType;
use crate::types::internal::SndInfoWire;
use os_socketaddr::OsSocketAddr;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, BorrowedFd};

/// One message of a batch to send: where it goes on a one-to-many socket, its octets and what
/// it is sent with.
pub(crate) struct Outgoing<'a> {
    pub(crate) to: Option<SocketAddr>,
    pub(crate) payload: &'a [u8],
    pub(crate) info: SendInfo,
}

/// The size of the send buffer of a socket and what of it is free, in octets as the kernel
/// counts them, which is more than the octets of the messages: `SO_MEMINFO`.
pub(crate) fn send_buffer(fd: BorrowedFd<'_>) -> std::io::Result<(usize, usize)> {
    use crate::consts::{SK_MEMINFO_SNDBUF, SK_MEMINFO_VARS, SK_MEMINFO_WMEM_QUEUED};
    // Safety: any octets are a valid array of integers.
    let info: [u32; SK_MEMINFO_VARS] = unsafe {
        super::getsockopt_internal(fd, libc::SOL_SOCKET, libc::SO_MEMINFO, [0; SK_MEMINFO_VARS])?
    };
    let size = info[SK_MEMINFO_SNDBUF] as usize;
    Ok((
        size,
        size.saturating_sub(info[SK_MEMINFO_WMEM_QUEUED] as usize),
    ))
}

/// Room for one `SCTP_SNDINFO` control message, aligned for `cmsghdr`.
#[repr(C)]
#[derive(Clone, Copy)]
struct SndInfoControl {
    _align: [libc::cmsghdr; 0],
    bytes: [u8; 64],
}

/// Send `messages` with one `sendmmsg`, each with its own send information, and with
/// `MSG_MORE` when `more`: the kernel then holds the DATA chunks of their association to
/// bundle them with those of the next send, which has to come without it. The number of
/// messages that the kernel took, from the first; the error of the first otherwise.
pub(crate) fn sctp_sendmmsg_once(
    fd: BorrowedFd<'_>,
    messages: &[Outgoing<'_>],
    more: bool,
) -> std::io::Result<usize> {
    let count = messages.len().min(libc::c_uint::MAX as usize);
    let mut addresses: Vec<Option<OsSocketAddr>> = Vec::with_capacity(count);
    let mut iovecs: Vec<libc::iovec> = Vec::with_capacity(count);
    let mut controls = vec![
        SndInfoControl {
            _align: [],
            bytes: [0; 64]
        };
        count
    ];
    // Safety: `CMSG_SPACE` and `CMSG_LEN` only compute sizes.
    let (space, length) = unsafe {
        let size = std::mem::size_of::<SndInfoWire>() as u32;
        (libc::CMSG_SPACE(size) as usize, libc::CMSG_LEN(size))
    };
    assert!(space <= 64, "SCTP_SNDINFO fits in its control buffer");
    for (message, control) in messages[..count].iter().zip(&mut controls) {
        addresses.push(message.to.map(OsSocketAddr::from));
        iovecs.push(libc::iovec {
            iov_base: message.payload.as_ptr() as *mut libc::c_void,
            iov_len: message.payload.len(),
        });
        // Some platforms have private fields in `cmsghdr`.
        // Safety: an all-zero `cmsghdr` is valid, and the buffer has room for it and its data.
        unsafe {
            let mut header: libc::cmsghdr = std::mem::zeroed();
            header.cmsg_level = libc::IPPROTO_SCTP;
            header.cmsg_type = CmsgType::SndInfo as i32;
            header.cmsg_len = length as _;
            std::ptr::write(control.bytes.as_mut_ptr().cast::<libc::cmsghdr>(), header);
            // The payload protocol identifier goes in network byte order
            let wire = SndInfoWire::from(&message.info);
            let data = control.bytes.as_mut_ptr().add(libc::CMSG_LEN(0) as usize);
            std::ptr::copy_nonoverlapping(
                std::ptr::addr_of!(wire).cast::<u8>(),
                data,
                std::mem::size_of::<SndInfoWire>(),
            );
        }
    }
    let mut headers: Vec<libc::mmsghdr> = Vec::with_capacity(count);
    for index in 0..count {
        // Safety: an all-zero `mmsghdr` is valid; some platforms have private fields in it.
        let mut header: libc::mmsghdr = unsafe { std::mem::zeroed() };
        if let Some(address) = &addresses[index] {
            let address: &[u8] = address.as_ref();
            header.msg_hdr.msg_name = address.as_ptr() as *mut libc::c_void;
            header.msg_hdr.msg_namelen = address.len() as _;
        }
        header.msg_hdr.msg_iov = &mut iovecs[index];
        header.msg_hdr.msg_iovlen = 1;
        header.msg_hdr.msg_control = controls[index].bytes.as_mut_ptr() as *mut libc::c_void;
        header.msg_hdr.msg_controllen = space as _;
        headers.push(header);
    }
    // Report a closed association as `EPIPE` without raising `SIGPIPE`.
    let flags = libc::MSG_NOSIGNAL | if more { libc::MSG_MORE } else { 0 };
    // Safety: every pointer of the headers points into `addresses`, `iovecs`, `controls` or
    // the payloads, which outlive the call, with the lengths given beside them.
    let sent = unsafe {
        libc::sendmmsg(
            fd.as_raw_fd(),
            headers.as_mut_ptr(),
            count as libc::c_uint,
            flags as _,
        )
    };
    if sent < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(sent as usize)
    }
}

/// Octets of each slot that a batch of reads fills: a message longer than it comes in the
/// parts that the slots after it receive, as from reads one after the other.
pub(crate) const SLOT: usize = 4096;
/// The slots of a batch of reads.
pub(crate) const SLOTS: usize = 64;

/// The buffers of a batch of reads, which a thread keeps from one batch to the next, and
/// the headers of `recvmmsg` that point into them, made once.
pub(crate) struct Slots {
    octets: Vec<u8>,
    controls: Vec<ControlBuffer>,
    names: Vec<libc::sockaddr_storage>,
    iovecs: Vec<libc::iovec>,
    headers: Vec<libc::mmsghdr>,
    pieces: Vec<Piece>,
}

// Room for the control messages of one read, as `sctp_recvmsg_once` has it.
#[repr(C)]
#[derive(Clone, Copy)]
struct ControlBuffer {
    _align: [libc::cmsghdr; 0],
    bytes: [u8; 256],
}

impl Slots {
    pub(crate) fn new() -> Self {
        let mut slots = Self {
            octets: vec![0; SLOT * SLOTS],
            controls: vec![
                ControlBuffer {
                    _align: [],
                    bytes: [0; 256]
                };
                SLOTS
            ],
            // Safety: an all-zero `sockaddr_storage` is valid.
            names: vec![unsafe { std::mem::zeroed() }; SLOTS],
            iovecs: Vec::with_capacity(SLOTS),
            headers: Vec::with_capacity(SLOTS),
            pieces: Vec::with_capacity(SLOTS),
        };
        // The vectors are never resized: what the headers point to stays where it is.
        for slot in slots.octets.as_chunks_mut::<SLOT>().0 {
            slots.iovecs.push(libc::iovec {
                iov_base: slot.as_mut_ptr() as *mut libc::c_void,
                iov_len: SLOT,
            });
        }
        let slots_of_headers = slots
            .iovecs
            .iter_mut()
            .zip(&mut slots.names)
            .zip(&mut slots.controls);
        for ((iovec, name), control) in slots_of_headers {
            // Safety: an all-zero `mmsghdr` is valid; some platforms have private fields in it.
            let mut header: libc::mmsghdr = unsafe { std::mem::zeroed() };
            header.msg_hdr.msg_name = std::ptr::from_mut(name).cast::<libc::c_void>();
            header.msg_hdr.msg_iov = iovec;
            header.msg_hdr.msg_iovlen = 1;
            header.msg_hdr.msg_control = control.bytes.as_mut_ptr() as *mut libc::c_void;
            slots.headers.push(header);
        }
        slots
    }

    /// The reads of the last batch.
    pub(crate) fn pieces(&self) -> &[Piece] {
        &self.pieces
    }

    /// The octets that read `index` of the last batch received.
    pub(crate) fn octets(&self, index: usize, piece: &Piece) -> &[u8] {
        &self.octets[index * SLOT..index * SLOT + piece.len]
    }
}

/// Up to `limit` reads with one `recvmmsg`, without waiting, into `slots`: the parts of
/// messages and the notifications that the kernel holds, each in a slot, in their order, as
/// `Slots::pieces` has them. How many; `WouldBlock` when it holds none.
pub(crate) fn sctp_recvmmsg_once(
    fd: BorrowedFd<'_>,
    slots: &mut Slots,
    limit: usize,
) -> std::io::Result<usize> {
    let count = limit.clamp(1, SLOTS);
    slots.pieces.clear();
    // The kernel writes the lengths of the name and of the control messages of each read.
    for header in &mut slots.headers[..count] {
        header.msg_hdr.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as _;
        header.msg_hdr.msg_controllen = 256;
    }
    // Safety: every pointer of the headers points into `slots`, whose vectors are never
    // resized, with the lengths given beside them. No timeout: the socket does not block.
    let received = unsafe {
        libc::recvmmsg(
            fd.as_raw_fd(),
            slots.headers.as_mut_ptr(),
            count as libc::c_uint,
            libc::MSG_DONTWAIT as _,
            std::ptr::null_mut(),
        )
    };
    if received < 0 {
        return Err(std::io::Error::last_os_error());
    }
    for (index, header) in slots.headers[..received as usize].iter().enumerate() {
        let message = &header.msg_hdr;
        // `msg_controllen` is not a `usize` on every target.
        #[allow(clippy::unnecessary_cast)]
        let control_len = (message.msg_controllen as usize).min(256);
        let (rcv_info, nxt_info) = ancillary_info(
            &slots.controls[index].bytes[..control_len],
            message.msg_flags,
        );
        // Safety: the kernel wrote `msg_namelen` octets of an address into the storage, which
        // has room for every address.
        let from = unsafe {
            OsSocketAddr::copy_from_raw(
                std::ptr::addr_of!(slots.names[index]).cast::<libc::sockaddr>(),
                message.msg_namelen,
            )
        }
        .into_addr();
        slots.pieces.push(Piece {
            len: header.msg_len as usize,
            notification: message.msg_flags as u32 & MSG_NOTIFICATION != 0,
            end_of_record: message.msg_flags & libc::MSG_EOR != 0,
            rcv_info,
            nxt_info,
            from,
        });
    }
    Ok(received as usize)
}
