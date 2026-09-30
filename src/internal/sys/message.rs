//! Individual message syscalls and their initialized ancillary buffers.
use crate::consts::MSG_NOTIFICATION;
use crate::internal::receive::Piece;
use crate::{CmsgType, NxtInfo, RcvInfo, SendInfo};
use os_socketaddr::OsSocketAddr;
use std::convert::TryInto;
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
        let (rcv_info, nxt_info) =
            ancillary_info(&msg_control.bytes[..control_len], recvmsg_header.msg_flags);
        let from = OsSocketAddr::copy_from_raw(
            std::ptr::addr_of!(from) as *const libc::sockaddr,
            recvmsg_header.msg_namelen,
        )
        .into_addr();
        Ok(Piece {
            len: result as usize,
            notification: recvmsg_header.msg_flags as u32 & MSG_NOTIFICATION != 0,
            end_of_record: recvmsg_header.msg_flags & libc::MSG_EOR != 0,
            rcv_info,
            nxt_info,
            from,
        })
    }
}

fn ancillary_info(mut control: &[u8], flags: libc::c_int) -> (Option<RcvInfo>, Option<NxtInfo>) {
    if flags & libc::MSG_CTRUNC != 0 {
        log::warn!("Control messages truncated, `RcvInfo` or `NxtInfo` may be missing.");
    }

    let mut rcv_info = None;
    let mut nxt_info = None;
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
            && header.cmsg_type == CmsgType::RcvInfo as i32
            && data.len() >= std::mem::size_of::<RcvInfo>()
        {
            // Safety: RcvInfo consists of integers, and its complete native layout is present.
            rcv_info = Some(unsafe { std::ptr::read_unaligned(data.as_ptr().cast::<RcvInfo>()) });
        } else if header.cmsg_level == libc::IPPROTO_SCTP
            && header.cmsg_type == CmsgType::NxtInfo as i32
            && data.len() >= std::mem::size_of::<NxtInfo>()
        {
            // Safety: NxtInfo consists of integers, and its complete native layout is present.
            nxt_info = Some(unsafe { std::ptr::read_unaligned(data.as_ptr().cast::<NxtInfo>()) });
        }
        // libc supplies the native alignment; CMSG_SPACE is only a size calculation. The
        // containing receive buffer is bounded, so its validated payload size fits c_uint.
        let next = unsafe { libc::CMSG_SPACE((len - header_len) as u32) as usize };
        if next > control.len() {
            break;
        }
        control = &control[next..];
    }
    (rcv_info, nxt_info)
}

pub(crate) fn sctp_sendmsg_once(
    fd: BorrowedFd<'_>,
    to: Option<SocketAddr>,
    payload: &[u8],
    snd_info: Option<&SendInfo>,
) -> std::io::Result<()> {
    // Safety: All the pointers are valid because they are within the current scope.
    // Also, this is just a wrapper over `libc` call.
    unsafe {
        let mut send_iov = libc::iovec {
            iov_base: payload.as_ptr() as *mut libc::c_void,
            iov_len: payload.len(),
        };

        // We have to create this `os_sockaddr` outside the `if let ...`
        // Else it will go out of scope and we'll be using it's raw pointer.
        let os_sockaddr: OsSocketAddr;
        let (to_buffer, to_buffer_len) = if let Some(addr) = to {
            os_sockaddr = addr.into();
            let slice: &[u8] = os_sockaddr.as_ref();
            (slice.as_ptr() as *mut _, os_sockaddr.len())
        } else {
            (std::ptr::null::<OsSocketAddr>() as *mut libc::c_void, 0)
        };
        // TODO: Support copy and other send info as well.
        let mut msg_control_buffer = CmsgBuffer::new();

        let (msg_control, msg_control_size) = if snd_info.is_some() {
            // Safety: wrapper over `libc` call. the size of the structures are wellknown.

            (
                msg_control_buffer.as_mut_ptr(),
                libc::CMSG_SPACE(std::mem::size_of::<SendInfo>() as u32) as usize,
            )
        } else {
            (
                std::ptr::null::<libc::cmsghdr>() as *mut libc::c_void,
                0_usize,
            )
        };

        // Some platforms have private fields in `msghdr`.
        let mut sendmsg_header: libc::msghdr = std::mem::zeroed();
        sendmsg_header.msg_name = to_buffer;
        sendmsg_header.msg_namelen = to_buffer_len;
        sendmsg_header.msg_iov = &mut send_iov;
        sendmsg_header.msg_iovlen = 1;
        sendmsg_header.msg_control = msg_control;
        sendmsg_header.msg_controllen = msg_control_size as _;

        let cmsg_hdr = libc::CMSG_FIRSTHDR(&sendmsg_header);
        if !cmsg_hdr.is_null() {
            (*cmsg_hdr).cmsg_level = libc::IPPROTO_SCTP;
            (*cmsg_hdr).cmsg_type = CmsgType::SndInfo as i32;
            (*cmsg_hdr).cmsg_len =
                libc::CMSG_LEN(std::mem::size_of::<SendInfo>().try_into().unwrap())
                    .try_into()
                    .unwrap();

            let snd_info = snd_info.unwrap();
            std::ptr::copy(
                snd_info as *const SendInfo as *const u8,
                libc::CMSG_DATA(cmsg_hdr),
                std::mem::size_of::<SendInfo>(),
            );
        }

        // Report a closed association as `EPIPE` without raising `SIGPIPE`.
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let flags = libc::MSG_NOSIGNAL;

        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        let flags = 0 as libc::c_int;

        let result = libc::sendmsg(
            fd.as_raw_fd(),
            &mut sendmsg_header as *mut libc::msghdr,
            flags,
        );
        if result < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

// Size of the buffer for control messages. It has room for those `recvmsg` can return for SCTP,
// `SCTP_NXTINFO`, `SCTP_RCVINFO` and `SCTP_SNDRCV` (with the data I/O event), after `SOL_SOCKET`
// ones enabled on the socket, such as timestamps.
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
        let header = libc::cmsghdr {
            cmsg_len: (header_len + data.len()) as _,
            cmsg_level: level,
            cmsg_type: kind,
        };
        // Safety: the allocated slice contains the native header, which contains integers only.
        unsafe {
            std::ptr::write_unaligned(bytes.as_mut_ptr().cast::<libc::cmsghdr>(), header);
        }
        bytes[header_len..header_len + data.len()].copy_from_slice(data);
        bytes
    }

    fn rcv_bytes(info: &RcvInfo) -> Vec<u8> {
        let mut bytes = vec![0; std::mem::size_of::<RcvInfo>()];
        macro_rules! put {
            ($field:ident) => {{
                let value = info.$field.to_ne_bytes();
                let offset = std::mem::offset_of!(RcvInfo, $field);
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

    fn nxt_bytes(info: &NxtInfo) -> Vec<u8> {
        let mut bytes = vec![0; std::mem::size_of::<NxtInfo>()];
        macro_rules! put {
            ($field:ident) => {{
                let value = info.$field.to_ne_bytes();
                let offset = std::mem::offset_of!(NxtInfo, $field);
                bytes[offset..offset + value.len()].copy_from_slice(&value);
            }};
        }
        put!(sid);
        put!(flags);
        put!(ppid);
        put!(length);
        put!(assoc_id);
        bytes
    }

    #[test]
    fn ancillary_bytes_preserve_metadata_across_other_levels_and_unaligned_input() {
        let rcv = RcvInfo {
            sid: 2,
            ssn: 3,
            flags: 1,
            ppid: 60_u32.to_be(),
            tsn: 4,
            cumtsn: 5,
            context: 6,
            assoc_id: 7,
        };
        let nxt = NxtInfo {
            sid: 8,
            flags: 1,
            ppid: 18_u32.to_be(),
            length: 10,
            assoc_id: 11,
        };
        let mut bytes = vec![0xff]; // force an unaligned beginning for the checked parser
        bytes.extend(cmsg(libc::SOL_SOCKET, 999, &[1, 2, 3]));
        bytes.extend(cmsg(
            libc::IPPROTO_SCTP,
            CmsgType::RcvInfo as _,
            &rcv_bytes(&rcv),
        ));
        bytes.extend(cmsg(libc::IPPROTO_SCTP, 999, &[4]));
        bytes.extend(cmsg(
            libc::IPPROTO_SCTP,
            CmsgType::NxtInfo as _,
            &nxt_bytes(&nxt),
        ));
        assert_eq!(
            ancillary_info(&bytes[1..], 0),
            (Some(rcv.clone()), Some(nxt.clone()))
        );
        assert_eq!(
            ancillary_info(&bytes[1..], libc::MSG_CTRUNC),
            (Some(rcv), Some(nxt))
        );
    }

    #[test]
    fn ancillary_bytes_stop_at_invalid_lengths_and_ignore_short_payloads() {
        let rcv = RcvInfo {
            assoc_id: 7,
            ..Default::default()
        };
        let valid = cmsg(libc::IPPROTO_SCTP, CmsgType::RcvInfo as _, &rcv_bytes(&rcv));
        for len in 0..valid.len() {
            let parsed = ancillary_info(&valid[..len], 0);
            let required =
                unsafe { libc::CMSG_LEN(std::mem::size_of::<RcvInfo>() as u32) as usize };
            assert_eq!(
                parsed,
                if len >= required {
                    (Some(rcv.clone()), None)
                } else {
                    (None, None)
                }
            );
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
            assert_eq!(ancillary_info(&invalid, 0), (None, None));
        }
        let mut bytes = cmsg(libc::IPPROTO_SCTP, CmsgType::RcvInfo as _, &[0; 3]);
        bytes.extend(valid);
        assert_eq!(ancillary_info(&bytes, 0), (Some(rcv), None));
    }

    #[test]
    fn cmsg_buffer_holds_the_sctp_control_messages() {
        // Safety: `CMSG_SPACE` only computes a size.
        let sctp_cmsgs = unsafe {
            libc::CMSG_SPACE(std::mem::size_of::<NxtInfo>() as u32)
                + libc::CMSG_SPACE(std::mem::size_of::<RcvInfo>() as u32)
                // `struct sctp_sndrcvinfo`
                + libc::CMSG_SPACE(32)
        };
        // Half of the buffer is left for `SOL_SOCKET` control messages.
        assert!((sctp_cmsgs as usize) <= CMSG_BUFFER_SIZE / 2);
        assert_eq!(
            std::mem::align_of::<CmsgBuffer>(),
            std::mem::align_of::<libc::cmsghdr>()
        );
    }
}
