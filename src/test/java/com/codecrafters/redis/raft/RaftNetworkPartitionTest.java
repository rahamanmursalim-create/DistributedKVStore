package com.codecrafters.redis.raft;

import com.codecrafters.redis.storage.RedisStore;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Deterministic bidirectional network cuts around independent Raft RPC listeners. */
public final class RaftNetworkPartitionTest {
    private static final class Link implements AutoCloseable {
        final int port, targetPort, from, to;
        final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
        final ExecutorService workers = Executors.newCachedThreadPool(r -> { Thread t = new Thread(r, "raft-partition-link"); t.setDaemon(true); return t; });
        volatile boolean enabled = true;
        volatile boolean running = true;
        ServerSocket listener;
        Thread acceptor;
        Link(int port, int targetPort, int from, int to) { this.port = port; this.targetPort = targetPort; this.from = from; this.to = to; }
        void start() throws IOException {
            listener = new ServerSocket(); listener.setReuseAddress(true); listener.bind(new InetSocketAddress("127.0.0.1", port));
            acceptor = new Thread(() -> { while (running) try {
                Socket incoming = listener.accept();
                if (!enabled) { incoming.close(); continue; }
                Socket outgoing = new Socket("127.0.0.1", targetPort);
                sockets.add(incoming); sockets.add(outgoing);
                workers.execute(() -> pipe(incoming, outgoing)); workers.execute(() -> pipe(outgoing, incoming));
            } catch (IOException e) { if (running && enabled) { /* a transient connect failure is a dropped RPC */ } } }, "raft-partition-accept");
            acceptor.setDaemon(true); acceptor.start();
        }
        void cut() { enabled = false; closeSockets(); }
        void heal() { enabled = true; }
        void pipe(Socket from, Socket to) {
            try { from.getInputStream().transferTo(to.getOutputStream()); } catch (IOException ignored) { }
            finally { closePair(from, to); }
        }
        void closePair(Socket a, Socket b) { try { a.close(); } catch (IOException ignored) { } try { b.close(); } catch (IOException ignored) { } sockets.remove(a); sockets.remove(b); }
        void closeSockets() { for (Socket socket : sockets) try { socket.close(); } catch (IOException ignored) { } sockets.clear(); }
        public void close() { running = false; enabled = false; try { listener.close(); } catch (Exception ignored) { } closeSockets(); workers.shutdownNow(); }
    }

    public static void main(String[] args) throws Exception {
        Path root = Files.createTempDirectory("raft-partition-test");
        int[] nodePorts = {port(), port(), port()};
        int[][] proxyPorts = new int[3][3];
        List<Link> links = new ArrayList<>();
        RaftGroup[] groups = new RaftGroup[3];
        RedisStore[] stores = new RedisStore[3];
        try {
            for (int from = 0; from < 3; from++) for (int to = 0; to < 3; to++) if (from != to) {
                proxyPorts[from][to] = port();
                Link link = new Link(proxyPorts[from][to], nodePorts[to], from, to); link.start(); links.add(link);
            }
            for (int i = 0; i < 3; i++) {
                stores[i] = new RedisStore(root.resolve("n" + i + ".wal"), false);
                Map<String, InetSocketAddress> peers = new HashMap<>();
                for (int j = 0; j < 3; j++) if (i != j) peers.put("n" + (j + 1), new InetSocketAddress("127.0.0.1", proxyPorts[i][j]));
                groups[i] = new RaftGroup("partition", "n" + (i + 1), new InetSocketAddress("127.0.0.1", nodePorts[i]), peers, root.resolve("n" + i), stores[i]);
                groups[i].start();
            }
            int leader = awaitLeader(groups, 10);
            if (leader < 0) throw new AssertionError("No leader before partition");
            int isolated = (leader + 1) % 3;
            cutNode(links, isolated);
            groups[leader].put("majority", "accepted".getBytes());
            awaitValue(groups[(leader + 2) % 3], "majority", "accepted", 5);
            boolean minorityRejected = false;
            try { groups[isolated].put("minority", "must-not-commit".getBytes()); } catch (IOException expected) { minorityRejected = true; }
            if (!minorityRejected) throw new AssertionError("Isolated minority node committed a write");
            heal(links);
            awaitValue(groups[isolated], "majority", "accepted", 8);

            // Remove every inter-node path. Even a node still claiming leadership must not ACK a write.
            for (Link link : links) link.cut();
            Thread.sleep(250);
            long before = groups[leader].getCommitIndex();
            boolean noQuorumRejected = false;
            try { groups[leader].put("no-quorum", "must-not-commit".getBytes()); } catch (IOException expected) { noQuorumRejected = true; }
            if (!noQuorumRejected || groups[leader].getCommitIndex() != before)
                throw new AssertionError("A partition without quorum acknowledged/committed a write");
            heal(links);
            awaitLeader(groups, 8);
            System.out.println("RaftNetworkPartitionTest passed: minority isolation, majority progress, heal/catch-up, no-quorum rejection");
        } finally {
            for (RaftGroup group : groups) if (group != null) group.close();
            for (RedisStore store : stores) if (store != null) store.close();
            for (Link link : links) link.close();
            try (var paths = Files.walk(root)) { paths.sorted(java.util.Comparator.reverseOrder()).forEach(p -> { try { Files.deleteIfExists(p); } catch (IOException ignored) { } }); }
        }
    }

    private static void cutNode(List<Link> links, int node) { for (Link link : links) if (link.from == node || link.to == node) link.cut(); }
    private static void heal(List<Link> links) { links.forEach(Link::heal); }
    private static void awaitValue(RaftGroup group, String key, String expected, int seconds) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (System.nanoTime() < deadline) {
            try { byte[] value = group.getWithReadIndex(key); if (value != null && expected.equals(new String(value))) return; }
            catch (IOException ignored) { }
            Thread.sleep(25);
        }
        throw new AssertionError("Value did not become visible on replica: " + key);
    }
    private static int awaitLeader(RaftGroup[] groups, int seconds) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (System.nanoTime() < deadline) { for (int i = 0; i < groups.length; i++) if (groups[i].isLeader()) return i; Thread.sleep(20); }
        return -1;
    }
    private static int port() throws IOException { try (ServerSocket socket = new ServerSocket(0)) { return socket.getLocalPort(); } }
}
