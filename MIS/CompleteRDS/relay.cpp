/*
 * relay.cpp — Thin bidirectional TCP relay (cn04/cn05)
 *
 * Forwards all traffic between CPU (login node) and DPU (192.168.100.2).
 * Usage: ./relay <listen_port> <dpu_host> <dpu_port>
 * Example: ./relay 9100 192.168.100.2 12345
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <poll.h>
#include <errno.h>

static int create_server(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); exit(1);
    }
    listen(fd, 1);
    return fd;
}

static int connect_to(const char *host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host, &addr.sin_addr);
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect to DPU"); exit(1);
    }
    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    int bufsz = 4 * 1024 * 1024;
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &bufsz, sizeof(bufsz));
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &bufsz, sizeof(bufsz));
    return fd;
}

static void relay_loop(int cpu_fd, int dpu_fd) {
    char buf[64 * 1024];
    struct pollfd fds[2];
    fds[0].fd = cpu_fd; fds[0].events = POLLIN;
    fds[1].fd = dpu_fd; fds[1].events = POLLIN;

    while (true) {
        int ret = poll(fds, 2, -1);
        if (ret < 0) { perror("poll"); break; }

        for (int i = 0; i < 2; i++) {
            if (fds[i].revents & POLLIN) {
                int src = fds[i].fd;
                int dst = (i == 0) ? dpu_fd : cpu_fd;
                ssize_t n = read(src, buf, sizeof(buf));
                if (n <= 0) {
                    printf("[Relay] Connection closed (side %d)\n", i);
                    return;
                }
                ssize_t sent = 0;
                while (sent < n) {
                    ssize_t w = write(dst, buf + sent, n - sent);
                    if (w <= 0) {
                        printf("[Relay] Write failed (side %d)\n", 1 - i);
                        return;
                    }
                    sent += w;
                }
            }
            if (fds[i].revents & (POLLERR | POLLHUP)) {
                printf("[Relay] Connection error/hangup (side %d)\n", i);
                return;
            }
        }
    }
}

int main(int argc, char **argv) {
    if (argc != 4) {
        fprintf(stderr, "Usage: %s <listen_port> <dpu_host> <dpu_port>\n", argv[0]);
        return 1;
    }
    int listen_port = atoi(argv[1]);
    const char *dpu_host = argv[2];
    int dpu_port = atoi(argv[3]);

    int server_fd = create_server(listen_port);
    printf("[Relay] Listening on port %d, will forward to %s:%d\n", listen_port, dpu_host, dpu_port);
    fflush(stdout);

    while (true) {
        printf("[Relay] Waiting for CPU connection...\n");
        fflush(stdout);

        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int cpu_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
        if (cpu_fd < 0) { perror("accept"); continue; }

        int opt = 1;
        setsockopt(cpu_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
        int bufsz = 4 * 1024 * 1024;
        setsockopt(cpu_fd, SOL_SOCKET, SO_SNDBUF, &bufsz, sizeof(bufsz));
        setsockopt(cpu_fd, SOL_SOCKET, SO_RCVBUF, &bufsz, sizeof(bufsz));

        printf("[Relay] CPU connected, connecting to DPU at %s:%d...\n", dpu_host, dpu_port);
        fflush(stdout);

        int dpu_fd = connect_to(dpu_host, dpu_port);
        printf("[Relay] Connected to DPU. Relaying...\n");
        fflush(stdout);

        relay_loop(cpu_fd, dpu_fd);

        close(cpu_fd);
        close(dpu_fd);
        printf("[Relay] Session ended. Waiting for next connection.\n");
        fflush(stdout);
    }

    close(server_fd);
    return 0;
}
