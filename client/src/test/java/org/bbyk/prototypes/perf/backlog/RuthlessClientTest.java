package org.bbyk.prototypes.perf.backlog;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author bbyk
 */
public class RuthlessClientTest {
    private final static Logger logger = LoggerFactory.getLogger(RuthlessClientTest.class);
    private final static int numOfCpus = Integer.getInteger("numOfCpus", 4);
    private final static Charset encoding = Charset.forName("utf8");
    private final static String serverHost = System.getProperty("serverHost", "localhost");
    private final static int serverPort = Integer.getInteger("serverPort", 8080);
    private final static InetSocketAddress serverAddr = new InetSocketAddress(serverHost, serverPort);
    private final static String endPointPath = System.getProperty("endPointPath", "/");
    private final static ByteBuffer tempaterRequestBuffer = encoding.encode(CharBuffer.wrap("GET " + endPointPath + " HTTP/1.0\r\n\r\n"));

    @Test
    public void stableRateConcurrentUsers() throws Exception {
        // epoll or kqueue is behind
        final ExecutorService ioLoopThreadPool = Executors.newFixedThreadPool(numOfCpus);
        final LinkedBlockingQueue<ConnectionData> connectionDatas = new LinkedBlockingQueue<ConnectionData>();
        final int numberOfRequests = 400;
        final CountDownLatch allDone = new CountDownLatch(numberOfRequests);
        final Selector[] selectors = new Selector[numOfCpus];
        final CountDownLatch allStarted = new CountDownLatch(numOfCpus);

        for (int i = 0; i < numOfCpus; i++) {
            final int selectorId = i;
            ioLoopThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        final Selector selector = Selector.open();
                        selectors[selectorId] = selector;
                        allStarted.countDown();

                        final List<ConnectionData> newConnections = Lists.newArrayList();

                        while (true) {
                            final int selected = selector.select();
                            logger.info("selected: " + selected);


                            newConnections.clear();
                            connectionDatas.drainTo(newConnections);

                            for (final ConnectionData connectionData : newConnections) {
                                final SelectionKey selectionKey = connectionData.socketChannel.register(selector, SelectionKey.OP_CONNECT);
                                selectionKey.attach(connectionData);
                            }

                            final Set<SelectionKey> selectionKeys = selector.selectedKeys();
                            logger.info("selected keys: " + selectionKeys.size());

                            final Iterator<SelectionKey> selectionKeyIterator = selectionKeys.iterator();
                            while (selectionKeyIterator.hasNext()) {
                                final SelectionKey selectionKey = selectionKeyIterator.next();
                                selectionKeyIterator.remove();

                                if (!selectionKey.isValid())
                                    continue;

                                if (selectionKey.isConnectable()) {
                                    try {
                                        final ConnectionData connectionData = (ConnectionData) selectionKey.attachment();
                                        if (!connectionData.socketChannel.finishConnect())
                                            logger.error("connection isn't established");

                                        selectionKey.interestOps(SelectionKey.OP_WRITE);
                                    } catch (IOException e) {
                                        logger.error("error finishing connection", e);
                                        allDone.countDown();
                                    }
                                } else if (selectionKey.isReadable()) {
                                    try {
                                        // The buffer into which we'll read data when it's available
                                        final ConnectionData connectionData = (ConnectionData) selectionKey.attachment();
                                        connectionData.readBuffer.clear();

                                        final int read = connectionData.socketChannel.read(connectionData.readBuffer);
                                        logger.info("read bytes: " + read);
                                        if (read == -1) {
                                            allDone.countDown();
                                            selectionKey.channel().close();
                                            selectionKey.cancel();
                                        } else {
                                            connectionData.readBuffer.flip();
                                            logger.info("response: " + encoding.decode(connectionData.readBuffer).toString());
                                        }

                                    } catch (IOException e) {
                                        logger.error("error reading data", e);
                                        allDone.countDown();
                                    }
                                } else if (selectionKey.isWritable()) {
                                    try {
                                        final ConnectionData connectionData = (ConnectionData) selectionKey.attachment();
                                        final int write = connectionData.socketChannel.write(connectionData.writeBuffer);
                                        logger.info("wrote bytes: " + write);

                                        selectionKey.interestOps(SelectionKey.OP_READ);
                                    } catch (IOException e) {
                                        logger.error("error writing to socket", e);
                                        allDone.countDown();
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.error("error in io loop", e);
                    } finally {
                        // it's all over let's unblock ourselves
                        for (int i = 0; i < allDone.getCount(); i++)
                            allDone.countDown();
                    }
                }
            });
        }

        allStarted.await();

        final Random random = new Random();
        for (int i = 0; i < numberOfRequests; i++) {
            // Create a non-blocking socket channel
            SocketChannel socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(false);

            final ConnectionData connectionData = new ConnectionData();
            connectionData.socketChannel = socketChannel;

            socketChannel.connect(serverAddr);
            connectionDatas.offer(connectionData);

            final int selectorIndex = random.nextInt(numOfCpus);
            logger.info("randomly selected: {}", selectorIndex);
            final Selector selector = selectors[selectorIndex];
            selector.wakeup();
        }


        allDone.await();

        // cleaning up
        ioLoopThreadPool.shutdown();
    }

    private static class ConnectionData {
        public ByteBuffer readBuffer = ByteBuffer.allocate(8192);
        public SocketChannel socketChannel;
        public ByteBuffer writeBuffer = tempaterRequestBuffer.asReadOnlyBuffer();
    }
}
