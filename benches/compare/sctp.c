/* The loops of compare.rs in C, on the sockets of the kernel: the floor for a binding of
 * them. `../compare.sh` builds it with `cc -O2 -pthread`.
 *
 *   sctp pingpong <octets> <messages>
 *   sctp stream <octets> <messages> <associations>
 *   sctp stream-many <octets> <messages> <associations>
 *
 * `pingpong` sends a message with `sendmsg` and SCTP_SNDINFO on one end of an association and
 * receives it with `recvmsg` and SCTP_RCVINFO on the other, on one thread. `stream` sends
 * the messages from one thread, in turns to each association, and receives them on another
 * with `epoll`; the sender keeps at most 4096 messages ahead. `stream-many` sends 64 to each
 * association in turn with one `sendmmsg`, all but the last with MSG_MORE, and receives with
 * `recvmmsg`, 64 at most. It allocates nothing in its loops. It prints what compare.rs
 * prints, with 0 allocations. */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/sctp.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define WINDOW 4096
#define BATCH 64
#define MOST 1024
#define RECEIVE_BUFFER (4 << 20)

static double now(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec * 1e9 + t.tv_nsec;
}

static void fail(const char *what) {
    perror(what);
    exit(1);
}

/* A connected pair on 127.0.0.1: the client sends, without Nagle's algorithm. */
static void pair(int *client, int *server) {
    int listener = socket(AF_INET, SOCK_STREAM, IPPROTO_SCTP), one = 1, size = RECEIVE_BUFFER;
    struct sockaddr_in address = {.sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
    socklen_t length = sizeof address;
    if (listener < 0 || bind(listener, (void *)&address, sizeof address) || listen(listener, 1) ||
        getsockname(listener, (void *)&address, &length))
        fail("listen");
    *client = socket(AF_INET, SOCK_STREAM, IPPROTO_SCTP);
    setsockopt(*client, IPPROTO_SCTP, SCTP_NODELAY, &one, sizeof one);
    setsockopt(*client, SOL_SOCKET, SO_RCVBUF, &size, sizeof size);
    if (connect(*client, (void *)&address, sizeof address)) fail("connect");
    *server = accept(listener, NULL, NULL);
    if (*server < 0) fail("accept");
    setsockopt(*server, IPPROTO_SCTP, SCTP_RECVRCVINFO, &one, sizeof one);
    setsockopt(*server, IPPROTO_SCTP, SCTP_NODELAY, &one, sizeof one);
    setsockopt(*server, SOL_SOCKET, SO_RCVBUF, &size, sizeof size);
    close(listener);
}

/* The control message of a send on stream 1 with the payload protocol identifier of NGAP. */
static void sndinfo(char *control, struct msghdr *message) {
    message->msg_control = control;
    message->msg_controllen = CMSG_SPACE(sizeof(struct sctp_sndinfo));
    struct cmsghdr *header = CMSG_FIRSTHDR(message);
    header->cmsg_level = IPPROTO_SCTP;
    header->cmsg_type = SCTP_SNDINFO;
    header->cmsg_len = CMSG_LEN(sizeof(struct sctp_sndinfo));
    struct sctp_sndinfo *info = (void *)CMSG_DATA(header);
    memset(info, 0, sizeof *info);
    info->snd_sid = 1;
    info->snd_ppid = htonl(60);
}

static char payload[1 << 17];

static void send_one(int socket, size_t octets) {
    char control[CMSG_SPACE(sizeof(struct sctp_sndinfo))];
    struct iovec iov = {payload, octets};
    struct msghdr message = {.msg_iov = &iov, .msg_iovlen = 1};
    sndinfo(control, &message);
    if (sendmsg(socket, &message, MSG_NOSIGNAL) < 0) fail("sendmsg");
}

/* A whole message into `buffer`, or -1 when the socket has nothing. */
static ssize_t receive_one(int socket, char *buffer, size_t room) {
    size_t got = 0;
    for (;;) {
        char control[256];
        struct iovec iov = {buffer + got, room - got};
        struct msghdr message = {.msg_iov = &iov, .msg_iovlen = 1, .msg_control = control,
                                 .msg_controllen = sizeof control};
        ssize_t read = recvmsg(socket, &message, 0);
        if (read < 0 && errno == EAGAIN && got == 0) return -1;
        if (read < 0 && errno == EAGAIN) continue;
        if (read <= 0) fail("recvmsg");
        got += read;
        if (message.msg_flags & MSG_EOR) return got;
    }
}

static long peak(void) {
    FILE *status = fopen("/proc/self/status", "r");
    char line[256];
    long kib = 0;
    while (status && fgets(line, sizeof line, status))
        if (sscanf(line, "VmHWM: %ld kB", &kib) == 1) break;
    if (status) fclose(status);
    return kib;
}

static void report(const char *name, const char *mode, size_t octets, long messages, int associations,
                   double started) {
    printf("%s %s %zu %ld %d: %.0f ns, 0.00 allocations, 0 octets, peak %ld KiB\n", name, mode, octets,
           messages, associations, (now() - started) / messages, peak());
}

static void pingpong(size_t octets, long messages) {
    int client, server;
    static char buffer[1 << 17];
    pair(&client, &server);
    for (int i = 0; i < 1000; i++) {
        send_one(client, octets);
        receive_one(server, buffer, sizeof buffer);
    }
    double started = now();
    for (long i = 0; i < messages; i++) {
        send_one(client, octets);
        if (receive_one(server, buffer, sizeof buffer) != (ssize_t)octets) fail("length");
    }
    report("c", "pingpong", octets, messages, 1, started);
}

/* What the receiver has received, which the sender waits on. */
static atomic_long received;
static atomic_int waiting;
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t progress = PTHREAD_COND_INITIALIZER;

static void wait_window(long sent) {
    if (sent <= atomic_load(&received) + WINDOW) return;
    pthread_mutex_lock(&lock);
    atomic_store(&waiting, 1);
    while (sent > atomic_load(&received) + WINDOW) pthread_cond_wait(&progress, &lock);
    atomic_store(&waiting, 0);
    pthread_mutex_unlock(&lock);
}

static void count_received(long count) {
    atomic_fetch_add(&received, count);
    if (atomic_load(&waiting)) {
        pthread_mutex_lock(&lock);
        pthread_cond_signal(&progress);
        pthread_mutex_unlock(&lock);
    }
}

struct receiver {
    int *servers, associations, many;
    long messages;
    size_t octets;
};

static void *receive_all(void *argument) {
    struct receiver *receiver = argument;
    static char buffer[1 << 17];
    static char slots[BATCH][4096], controls[BATCH][256];
    int poll = epoll_create1(0);
    for (int i = 0; i < receiver->associations; i++) {
        struct epoll_event event = {.events = EPOLLIN, .data.fd = receiver->servers[i]};
        epoll_ctl(poll, EPOLL_CTL_ADD, receiver->servers[i], &event);
    }
    long total = 0;
    struct epoll_event events[64];
    while (total < receiver->messages) {
        int ready = epoll_wait(poll, events, 64, -1);
        for (int e = 0; e < ready; e++) {
            int socket = events[e].data.fd;
            if (!receiver->many) {
                ssize_t got;
                while ((got = receive_one(socket, buffer, sizeof buffer)) >= 0) {
                    if (got != (ssize_t)receiver->octets) fail("length");
                    total++;
                    count_received(1);
                }
                continue;
            }
            for (;;) {
                struct mmsghdr headers[BATCH];
                struct iovec iovs[BATCH];
                for (int i = 0; i < BATCH; i++) {
                    iovs[i] = (struct iovec){slots[i], sizeof slots[i]};
                    headers[i] = (struct mmsghdr){.msg_hdr = {.msg_iov = &iovs[i], .msg_iovlen = 1,
                                                              .msg_control = controls[i],
                                                              .msg_controllen = sizeof controls[i]}};
                }
                int got = recvmmsg(socket, headers, BATCH, 0, NULL);
                if (got < 0 && errno == EAGAIN) break;
                if (got <= 0) fail("recvmmsg");
                total += got;
                count_received(got);
                if (got < BATCH) break;
            }
        }
    }
    return NULL;
}

static void stream(size_t octets, long messages, int associations, int many) {
    int clients[MOST], servers[MOST];
    if (associations > MOST) fail("associations");
    for (int i = 0; i < associations; i++) {
        pair(&clients[i], &servers[i]);
        int flags = 1;
        if (ioctl(servers[i], FIONBIO, &flags)) fail("nonblocking");
    }
    struct receiver receiver = {servers, associations, many, messages, octets};
    pthread_t thread;
    double started = now();
    pthread_create(&thread, NULL, receive_all, &receiver);
    if (!many) {
        for (long i = 0; i < messages; i++) {
            wait_window(i);
            send_one(clients[i % associations], octets);
        }
    } else {
        char controls[BATCH][CMSG_SPACE(sizeof(struct sctp_sndinfo))];
        struct mmsghdr headers[BATCH];
        struct iovec iovs[BATCH];
        for (int i = 0; i < BATCH; i++) {
            iovs[i] = (struct iovec){payload, octets};
            headers[i] = (struct mmsghdr){.msg_hdr = {.msg_iov = &iovs[i], .msg_iovlen = 1}};
            sndinfo(controls[i], &headers[i].msg_hdr);
        }
        long left[MOST], sent = 0;
        for (int i = 0; i < associations; i++) left[i] = messages / associations + (i < messages % associations);
        while (sent < messages) {
            for (int i = 0; i < associations; i++) {
                if (left[i] == 0) continue;
                wait_window(sent);
                int count = left[i] < BATCH ? left[i] : BATCH;
                /* All but the last with MSG_MORE, which the last releases */
                int done = 0;
                if (count > 1) {
                    done = sendmmsg(clients[i], headers, count - 1, MSG_NOSIGNAL | MSG_MORE);
                    if (done < 0) fail("sendmmsg");
                }
                int last = sendmmsg(clients[i], headers + done, 1, MSG_NOSIGNAL);
                if (last < 0) fail("sendmmsg");
                left[i] -= done + last;
                sent += done + last;
            }
        }
    }
    pthread_join(thread, NULL);
    report("c", many ? "stream-many" : "stream", octets, messages, associations, started);
}

int main(int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr, "usage: sctp pingpong|stream|stream-many <octets> <messages> [associations]\n");
        return 2;
    }
    memset(payload, 0x5a, sizeof payload);
    size_t octets = strtoul(argv[2], NULL, 10);
    long messages = strtol(argv[3], NULL, 10);
    int associations = argc > 4 ? atoi(argv[4]) : 1;
    if (!strcmp(argv[1], "pingpong")) pingpong(octets, messages);
    else if (!strcmp(argv[1], "stream")) stream(octets, messages, associations, 0);
    else if (!strcmp(argv[1], "stream-many")) stream(octets, messages, associations, 1);
    else return 2;
    return 0;
}
