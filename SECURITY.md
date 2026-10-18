# Security policy

## Reporting a vulnerability

Report a vulnerability in private, through GitHub: on the
[Security tab](https://github.com/linouxis9/oxirush-sctp/security) of this
repository, choose **Report a vulnerability**. Do not open a public issue
for it.

Say in the report:

- the version or the commit of the crate, the Rust version, the target and
  the Linux version;
- the calls, and what the peer sends, that show the problem, as a test or a
  short program when you can;
- what happens and what you expected: a read or a write out of bounds, a
  descriptor that leaks or is closed twice, a message that is cut, joined to
  another or given to the wrong association, memory that a peer can make
  grow without bound.

The answer comes in the report itself. A fix is released as a new version
of the crate, with an advisory that credits you unless you ask otherwise.

There is no bug bounty.

## Supported versions

Fixes go into the latest release. An older version gets none.

## What to know before reporting

The crate calls the SCTP sockets of the Linux kernel through `libc`, in
`unsafe` code: most of it is under `src/internal/sys/`. The protocol itself
is the kernel's: report a defect of the SCTP implementation to the Linux
kernel. The crate has not had a security audit.
