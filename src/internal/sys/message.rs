//! Individual message syscalls and their initialized ancillary buffers.
use crate::consts::MSG_NOTIFICATION;
use crate::internal::receive::Piece;
use crate::types::internal::{RcvInfoWire, SndInfoWire};
use os_socketaddr::OsSocketAddr;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, BorrowedFd};

pub(crate) fn sctp_recvmsg_once(fd: BorrowedFd<'_>, buffer: &mut [u8]) -> std::io::Result<Piece> {
    // Safety: all the pointers in `recvmsg_header` point to buffers valid during the call, of the
    // lengths given with them.
    unsafe {
        let mut recv_iov = libc::iovec {
            iov_base: buffer.as_mut_ptr() as *mut libc::c_void,
            iov_len: buffer.len(),
        };
        let mut msg_control = CmsgBuffer::new();
        let mut from: libc::sockaddr_storage = std::mem::zeroed();

        // Some platforms have private fields in `msghdr`.
        let mut recvmsg_header: libc::msghdr = std::mem::zeroed();
        recvmsg_header.msg_name = std::ptr::addr_of_mut!(from) as *mut libc::c_void;
        recvmsg_header.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as _;
        recvmsg_header.msg_iov = &mut recv_iov;
        recvmsg_header.msg_iovlen = 1;
        recvmsg_header.msg_control = msg_control.as_mut_ptr();
        recvmsg_header.msg_controllen = CMSG_BUFFER_SIZE as _;

        let flags = 0 as libc::c_int;
        let result = libc::recvmsg(
            fd.as_raw_fd(),
            &mut recvmsg_header as *mut libc::msghdr,
            flags,
        );
        if result < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let control_len = (recvmsg_header.msg_controllen as usize).min(msg_control.bytes.len());
        let info = ancillary_info(&msg_control.bytes[..control_len]).unwrap_or_default();
        let from = OsSocketAddr::copy_from_raw(
            std::ptr::addr_of!(from) as *const libc::sockaddr,
            recvmsg_header.msg_namelen,
        )
        .into_addr();
        Ok(Piece {
            len: result as usize,
            notification: recvmsg_header.msg_flags as u32 & MSG_NOTIFICATION != 0,
            end_of_record: recvmsg_header.msg_flags & libc::MSG_EOR != 0,
            info,
            from,
        })
    }
}

// The receive information among the control messages of a read, if they have it.
pub(crate) fn ancillary_info(mut control: &[u8]) -> Option<RcvInfoWire> {
    let mut info = None;
    // Native headers contain integers only. Reading them unaligned after validating their
    // complete byte range avoids walking pointers supplied by a header's claimed length.
    let header_len = unsafe { libc::CMSG_LEN(0) as usize };
    while control.len() >= std::mem::size_of::<libc::cmsghdr>() {
        // Safety: the checked slice contains the full native header; all integer patterns are valid.
        let header = unsafe { std::ptr::read_unaligned(control.as_ptr().cast::<libc::cmsghdr>()) };
        let len = header.cmsg_len as usize;
        if len < header_len || len > control.len() {
            break;
        }
        let data = &control[header_len..len];
        if header.cmsg_level == libc::IPPROTO_SCTP
            && header.cmsg_type == crate::consts::SCTP_RCVINFO
            && data.len() >= std::mem::size_of::<RcvInfoWire>()
        {
            // Safety: the structure consists of integers, and its complete native layout is
            // present.
            info = Some(unsafe { std::ptr::read_unaligned(data.as_ptr().cast::<RcvInfoWire>()) });
        }
        // libc supplies the native alignment; CMSG_SPACE is only a size calculation. The
        // containing receive buffer is bounded, so its validated payload size fits c_uint.
        let next = unsafe { libc::CMSG_SPACE((len - header_len) as u32) as usize };
        if next > control.len() {
            break;
        }
        control = &control[next..];
    }
    info
}

// Sends `payload` with its send information, to `to` or to the association that `info`
// names.
pub(crate) fn sctp_sendmsg_once(
    fd: BorrowedFd<'_>,
    to: Option<SocketAddr>,
    payload: &[u8],
    info: SndInfoWire,
) -> std::io::Result<()> {
    let mut send_iov = libc::iovec {
        iov_base: payload.as_ptr() as *mut libc::c_void,
        iov_len: payload.len(),
    };
    let to = to.map(OsSocketAddr::from);
    let mut control = CmsgBuffer::new();
    // Safety: every pointer of the header points to a value of this scope, with the length
    // given beside it; the control buffer has room for the header and the send information.
    unsafe {
        // Some platforms have private fields in `msghdr`.
        let mut header: libc::msghdr = std::mem::zeroed();
        if let Some(to) = &to {
            header.msg_name = to.as_ptr() as *mut libc::c_void;
            header.msg_namelen = to.len();
        }
        header.msg_iov = &mut send_iov;
        header.msg_iovlen = 1;
        header.msg_control = control.as_mut_ptr();
        header.msg_controllen = libc::CMSG_SPACE(std::mem::size_of::<SndInfoWire>() as u32) as _;
        let cmsg = libc::CMSG_FIRSTHDR(&header);
        (*cmsg).cmsg_level = libc::IPPROTO_SCTP;
        (*cmsg).cmsg_type = crate::consts::SCTP_SNDINFO;
        (*cmsg).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<SndInfoWire>() as u32) as _;
        std::ptr::copy_nonoverlapping(
            std::ptr::addr_of!(info).cast::<u8>(),
            libc::CMSG_DATA(cmsg),
            std::mem::size_of::<SndInfoWire>(),
        );
        // Report a closed association as `EPIPE` without raising `SIGPIPE`.
        if libc::sendmsg(fd.as_raw_fd(), &header, libc::MSG_NOSIGNAL) < 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

// Size of the buffer for control messages. It has room for `SCTP_RCVINFO` after the
// `SOL_SOCKET` ones enabled on the socket, such as timestamps.
const CMSG_BUFFER_SIZE: usize = 256;

// A buffer for control messages, aligned for `cmsghdr`.
#[repr(C)]
struct CmsgBuffer {
    _align: [libc::cmsghdr; 0],
    bytes: [u8; CMSG_BUFFER_SIZE],
}

impl CmsgBuffer {
    fn new() -> Self {
        Self {
            _align: [],
            bytes: [0; CMSG_BUFFER_SIZE],
        }
    }

    fn as_mut_ptr(&mut self) -> *mut libc::c_void {
        self.bytes.as_mut_ptr() as *mut libc::c_void
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn cmsg(level: libc::c_int, kind: libc::c_int, data: &[u8]) -> Vec<u8> {
        let header_len = unsafe { libc::CMSG_LEN(0) as usize };
        let size = unsafe { libc::CMSG_SPACE(data.len() as u32) as usize };
        let mut bytes = vec![0; size];
        // Some platforms have private fields in `cmsghdr`.
        let mut header: libc::cmsghdr = unsafe { std::mem::zeroed() };
        header.cmsg_len = (header_len + data.len()) as _;
        header.cmsg_level = level;
        header.cmsg_type = kind;
        // Safety: the allocated slice contains the native header, which contains integers only.
        unsafe {
            std::ptr::write_unaligned(bytes.as_mut_ptr().cast::<libc::cmsghdr>(), header);
        }
        bytes[header_len..header_len + data.len()].copy_from_slice(data);
        bytes
    }

    // The structure as the kernel lays it out.
    fn rcv_bytes(info: &RcvInfoWire) -> Vec<u8> {
        let mut bytes = vec![0; std::mem::size_of::<RcvInfoWire>()];
        macro_rules! put {
            ($field:ident) => {{
                let value = info.$field.to_ne_bytes();
                let offset = std::mem::offset_of!(RcvInfoWire, $field);
                bytes[offset..offset + value.len()].copy_from_slice(&value);
            }};
        }
        put!(sid);
        put!(ssn);
        put!(flags);
        put!(ppid);
        put!(tsn);
        put!(cumtsn);
        put!(context);
        put!(assoc_id);
        bytes
    }

    #[test]
    fn ancillary_bytes_preserve_metadata_across_other_levels_and_unaligned_input() {
        let rcv = RcvInfoWire {
            sid: 2,
            ssn: 3,
            flags: 1,
            ppid: 60_u32.to_be(),
            tsn: 4,
            cumtsn: 5,
            context: 6,
            assoc_id: 7,
        };
        let mut bytes = vec![0xff]; // force an unaligned beginning for the checked parser
        bytes.extend(cmsg(libc::SOL_SOCKET, 999, &[1, 2, 3]));
        bytes.extend(cmsg(
            libc::IPPROTO_SCTP,
            crate::consts::SCTP_RCVINFO,
            &rcv_bytes(&rcv),
        ));
        bytes.extend(cmsg(libc::IPPROTO_SCTP, 999, &[4]));
        assert_eq!(ancillary_info(&bytes[1..]), Some(rcv));
    }

    #[test]
    fn ancillary_bytes_stop_at_invalid_lengths_and_ignore_short_payloads() {
        let rcv = RcvInfoWire {
            assoc_id: 7,
            ..Default::default()
        };
        let valid = cmsg(
            libc::IPPROTO_SCTP,
            crate::consts::SCTP_RCVINFO,
            &rcv_bytes(&rcv),
        );
        for len in 0..valid.len() {
            let parsed = ancillary_info(&valid[..len]);
            let required =
                unsafe { libc::CMSG_LEN(std::mem::size_of::<RcvInfoWire>() as u32) as usize };
            assert_eq!(parsed, (len >= required).then_some(rcv));
        }
        for claimed in [0_usize, 1, usize::MAX] {
            let mut invalid = valid.clone();
            // Safety: the full header is present and integer values have no invalid patterns.
            let mut header =
                unsafe { std::ptr::read_unaligned(invalid.as_ptr().cast::<libc::cmsghdr>()) };
            header.cmsg_len = claimed as _;
            unsafe {
                std::ptr::write_unaligned(invalid.as_mut_ptr().cast::<libc::cmsghdr>(), header);
            }
            assert_eq!(ancillary_info(&invalid), None);
        }
        let mut bytes = cmsg(libc::IPPROTO_SCTP, crate::consts::SCTP_RCVINFO, &[0; 3]);
        bytes.extend(valid);
        assert_eq!(ancillary_info(&bytes), Some(rcv));
    }

    /// A control message of a header alone is skipped, and so is one of another level or
    /// type, however long: neither ends the walk nor is taken for SCTP's.
    #[test]
    fn ancillary_bytes_skip_empty_and_foreign_control_messages() {
        let rcv = RcvInfoWire {
            assoc_id: 7,
            ..Default::default()
        };
        let mut bytes = cmsg(libc::IPPROTO_SCTP, crate::consts::SCTP_RCVINFO, &[]);
        bytes.extend(cmsg(libc::SOL_SOCKET, 1, &[0x5a; 40]));
        bytes.extend(cmsg(libc::IPPROTO_SCTP, 999, &[0x5a; 40]));
        assert_eq!(ancillary_info(&bytes), None);
        bytes.extend(cmsg(
            libc::IPPROTO_SCTP,
            crate::consts::SCTP_RCVINFO,
            &rcv_bytes(&rcv),
        ));
        assert_eq!(ancillary_info(&bytes), Some(rcv));
    }

    /// Control messages that the kernel truncated give what they have.
    #[test]
    fn truncated_control_messages_give_what_they_have() {
        assert_eq!(ancillary_info(&[]), None);
    }

    #[test]
    fn cmsg_buffer_holds_the_sctp_control_messages() {
        // Safety: `CMSG_SPACE` only computes a size.
        let sctp_cmsgs = unsafe { libc::CMSG_SPACE(std::mem::size_of::<RcvInfoWire>() as u32) };
        // Most of the buffer is left for `SOL_SOCKET` control messages.
        assert!((sctp_cmsgs as usize) <= CMSG_BUFFER_SIZE / 4);
        assert_eq!(
            std::mem::align_of::<CmsgBuffer>(),
            std::mem::align_of::<libc::cmsghdr>()
        );
    }
}
