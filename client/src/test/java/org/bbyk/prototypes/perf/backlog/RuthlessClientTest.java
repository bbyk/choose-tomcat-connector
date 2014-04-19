package org.bbyk.prototypes.perf.backlog;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.concurrent.BasicThreadFactory.Builder;
import org.apache.commons.lang3.mutable.MutableLong;
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
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author bbyk
 */
public class RuthlessClientTest {
    private static final int MILLISECONDS_PER_SECOND = 1000;

    private final static Logger logger = LoggerFactory.getLogger(RuthlessClientTest.class);
    private final static int bufferSize = Integer.getInteger("bufferSize", 8192);
    private final static int numOfCpus = Integer.getInteger("numOfCpus", Runtime.getRuntime().availableProcessors());
    private final static Charset encoding = Charset.forName("utf8");
    private final static String serverHost = System.getProperty("serverHost", "localhost");
    private final static int serverPort = Integer.getInteger("serverPort", 8081);
    private final static InetSocketAddress serverAddr = new InetSocketAddress(serverHost, serverPort);
    private final static String endPointPath = System.getProperty("endPointPath", "/server/endpoint");
    private final static ByteBuffer requestLineWriteBuffer = encoding.encode(CharBuffer.wrap("POST " + endPointPath + " HTTP/1.1\r\n"));
    private final static int postPayloadSize = Integer.getInteger("postPayloadSize", 4 * 1024);
    private final static ByteBuffer headersWriteBuffer = encoding.encode(CharBuffer.wrap("Host: " + serverHost + ":" + serverPort + "\r\nContent-Length: " + postPayloadSize + "\r\n\r\n"));
    private final static int requestsPerSecond = Integer.getInteger("requestsPerSecond", 1600);
    private final static int testDurationInSeconds = Integer.getInteger("testDuration", 100);
    private final static long tickIntervalMs = Integer.getInteger("tickIntervalMs", 10);
    private final static boolean verboseErrors = Boolean.getBoolean("verboseErrors");
    private final static boolean slowSendStart = Boolean.getBoolean("slowSendStart");
    private final static int slowSendStartPauseMs = Integer.getInteger("slowSendStartPauseMs", 100);
    private final static CharBuffer lineSeparatorCharBuffer = CharBuffer.wrap("\r\n");

    @Test
    public void stableRateConcurrentUsers() throws Exception {
        // generate random post payload
        final byte[] postPayLoad = new byte[postPayloadSize];
        new Random().nextBytes(postPayLoad);

        final int numberOfRequests = requestsPerSecond * testDurationInSeconds;
        logger.info("number of requests {}", numberOfRequests);
        final CountDownLatch allDone = new CountDownLatch(numberOfRequests);

        final Builder ioLoopThreadPoolThreadFactoryBuilder = new BasicThreadFactory.Builder();
        ioLoopThreadPoolThreadFactoryBuilder.namingPattern("ioLoop-%s");
        ioLoopThreadPoolThreadFactoryBuilder.daemon(true);
        final ExecutorService ioLoopThreadPool = new ThreadPoolExecutor(
                numOfCpus, numOfCpus,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                ioLoopThreadPoolThreadFactoryBuilder.build());

        final Builder loadBalancerThreadPoolThreadFactoryBuilder = new BasicThreadFactory.Builder();
        loadBalancerThreadPoolThreadFactoryBuilder.namingPattern("loadBalancer-%s");
        loadBalancerThreadPoolThreadFactoryBuilder.daemon(true);
        final ExecutorService loadBalancerThreadPool = new ThreadPoolExecutor(
                1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                loadBalancerThreadPoolThreadFactoryBuilder.build());

        final LinkedBlockingQueue<RequestData> requestsPendingToSend = new LinkedBlockingQueue<RequestData>();
        final Selector[] selectors = new Selector[numOfCpus];
        final CountDownLatch allStarted = new CountDownLatch(numOfCpus);
        final AtomicInteger errorCount = new AtomicInteger();
        final AtomicInteger connectedCount = new AtomicInteger();
        final AtomicInteger writtenCount = new AtomicInteger();
        final AtomicInteger readCount = new AtomicInteger();
        final Object syncRoot = new Object();
        final ConcurrentMap<RequestData, RequestData> currentlyExecutingRequests = Maps.newConcurrentMap();
        final AtomicInteger initiatedRequestsCount = new AtomicInteger();
        final AtomicInteger processedRequestsCount = new AtomicInteger();
        final AtomicLong openSocketTookMsMax = new AtomicLong();

        for (int ci = 0; ci < numOfCpus; ci++) {
            final int selectorId = ci;
            final PriorityQueue<TimeCallback> timers = new PriorityQueue<TimeCallback>(11, new Comparator<TimeCallback>() {
                @Override
                public int compare(TimeCallback o1, TimeCallback o2) {
                    return o1.scheduledAt < o2.scheduledAt ? -1 : (o1.scheduledAt == o2.scheduledAt ? 0 : 1);
                }
            });
            ioLoopThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        final Selector selector;

                        // Oracle didn't fix the bug for sun.nio.ch.Util.atBugLevel
                        // http://bugs.sun.com/view_bug.do?bug_id=6427854
                        synchronized (syncRoot) {
                            selector = Selector.open();
                        }
                        selectors[selectorId] = selector;
                        allStarted.countDown();

                        final List<RequestData> newRequests = Lists.newArrayList();

                        //noinspection InfiniteLoopStatement
                        while (true) {
                            final int selected = selector.select();

                            if (logger.isDebugEnabled())
                                logger.debug("selected: " + selected);

                            final Set<SelectionKey> selectionKeys = selector.selectedKeys();

                            if (logger.isDebugEnabled())
                                logger.debug("selected keys: " + selectionKeys.size());

                            newRequests.clear();
                            requestsPendingToSend.drainTo(newRequests);

                            // establish currentlyExecutingRequests for the new requests
                            for (final RequestData requestData : newRequests) {
                                final SelectionKey selectionKey = requestData.socketChannel.register(selector, SelectionKey.OP_CONNECT);
                                selectionKey.attach(requestData);
                                try {
                                    requestData.socketChannel.connect(serverAddr);
                                    currentlyExecutingRequests.putIfAbsent(requestData, requestData);
                                    initiatedRequestsCount.incrementAndGet();
                                } catch (IOException e) {
                                    if (verboseErrors)
                                        logger.error("error establishing connection", e);
                                    closeKeyOnError(selectionKey);
                                }
                            }

                            // run pending callbacks
                            while (true) {
                                final TimeCallback callback = timers.peek();
                                if (callback == null)
                                    break;
                                if (callback.scheduledAt > System.currentTimeMillis())
                                    break;

                                timers.poll(); // remove it
                                callback.callback.run();
                            }

                            final Iterator<SelectionKey> selectionKeyIterator = selectionKeys.iterator();
                            while (selectionKeyIterator.hasNext()) {
                                final SelectionKey selectionKey = selectionKeyIterator.next();
                                selectionKeyIterator.remove();

                                if (!selectionKey.isValid()) {
                                    closeKeyOnError(selectionKey);
                                    continue;
                                }

                                if (selectionKey.isConnectable()) {
                                    final RequestData requestData = (RequestData) selectionKey.attachment();
                                    try {
                                        if (!requestData.socketChannel.finishConnect()) {
                                            if (verboseErrors)
                                                logger.error("connection isn't established");
                                            closeKeyOnError(selectionKey);
                                            continue;
                                        }

                                        connectedCount.incrementAndGet();
                                        selectionKey.interestOps(SelectionKey.OP_WRITE);
                                    } catch (IOException e) {
                                        if (verboseErrors)
                                            logger.error("error finishing connection", e);
                                        closeKeyOnError(selectionKey);
                                    }
                                } else if (selectionKey.isReadable()) {
                                    final RequestData requestData = (RequestData) selectionKey.attachment();
                                    try {
                                        // The buffer into which we'll read data when it's available
                                        requestData.responseReadBuffer.clear();

                                        final int read = requestData.socketChannel.read(requestData.responseReadBuffer);
                                        if (logger.isDebugEnabled())
                                            logger.debug("read bytes: " + read);
                                        if (read == -1) {
                                            readCount.incrementAndGet();
                                            closeKey(selectionKey);
                                            requestData.finishRead();
                                        } else {
                                            requestData.readBytes.addAndGet(read);
                                            requestData.processReadBuffer();
                                        }
                                    } catch (IOException e) {
                                        if (verboseErrors)
                                            logger.error("error reading data", e);
                                        closeKeyOnError(selectionKey);
                                        currentlyExecutingRequests.remove(requestData);
                                    }
                                } else if (selectionKey.isWritable()) {
                                    final RequestData requestData = (RequestData) selectionKey.attachment();
                                    final Runnable writeClosure = new Runnable() {
                                        @Override
                                        public void run() {
                                            try {
                                                final ByteBuffer[] byteBuffers = {requestLineWriteBuffer.asReadOnlyBuffer(), headersWriteBuffer.asReadOnlyBuffer(), requestData.payloadWriteBuffer};
                                                long write = requestData.socketChannel.write(byteBuffers);
                                                if (logger.isDebugEnabled())
                                                    logger.debug("#1 written bytes: " + write);

                                                writtenCount.incrementAndGet();
                                                selectionKey.interestOps(SelectionKey.OP_READ);
                                            } catch (IOException e) {
                                                if (verboseErrors)
                                                    logger.error("error writing to socket", e);
                                                try {
                                                    closeKeyOnError(selectionKey);
                                                } catch (IOException e1) {
                                                    throw Throwables.propagate(e1);
                                                }
                                            }
                                        }
                                    };

                                    if (slowSendStart) {
                                        final TimeCallback timeCallback = new TimeCallback();
                                        timeCallback.scheduledAt = System.currentTimeMillis() + slowSendStartPauseMs;
                                        timeCallback.callback = writeClosure;
                                        timers.add(timeCallback);
                                    } else {
                                        writeClosure.run();
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.error("error in io loop", e);
                    } finally {
                        // it's all over let's unblock ourselves
                        while (allDone.getCount() > 0)
                            allDone.countDown();
                    }
                }

                private void closeKeyOnError(SelectionKey selectionKey) throws IOException {
                    closeKey(selectionKey);
                    errorCount.incrementAndGet();
                }

                private void closeKey(SelectionKey selectionKey) throws IOException {
                    final RequestData requestData = (RequestData) selectionKey.attachment();
                    allDone.countDown();
                    selectionKey.channel().close();
                    selectionKey.cancel();
                    currentlyExecutingRequests.remove(requestData);
                    processedRequestsCount.incrementAndGet();
                }
            });
        }

        allStarted.await();

        final MutableLong lastTickWorkTookMs = new MutableLong();
        final AtomicInteger requestsScheduled = new AtomicInteger();

        loadBalancerThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    long lastTickTs = System.currentTimeMillis();
                    int leftover = 0;
                    final Random random = new Random();
                    while (requestsScheduled.get() < numberOfRequests) {
                        final long effectiveTickIntervalMs = Math.max(0, tickIntervalMs - lastTickWorkTookMs.getValue());
                        if (effectiveTickIntervalMs > 0)
                            Thread.sleep(effectiveTickIntervalMs);

                        final long now = System.currentTimeMillis();
                        final long elapsed = now - lastTickTs;
                        final int effectiveElapsed = Ints.checkedCast(requestsPerSecond * elapsed) + leftover;
                        final int requestsToSend = Math.min(numberOfRequests - requestsScheduled.get(), effectiveElapsed / MILLISECONDS_PER_SECOND);

                        leftover = effectiveElapsed % MILLISECONDS_PER_SECOND;
                        lastTickTs = now;

                        if (requestsToSend == 0) {
                            lastTickWorkTookMs.setValue(System.currentTimeMillis() - now);
                            continue;
                        }

                        for (int j = 0; j < requestsToSend; j++) {
                            // Create a non-blocking socket channel
                            final long openSocketNow = System.currentTimeMillis();
                            final SocketChannel socketChannel = SocketChannel.open();
                            setIfGreater(openSocketTookMsMax, System.currentTimeMillis() - openSocketNow);

                            if (logger.isDebugEnabled())
                                logger.debug(String.format("default buffer sizes: %d %d", socketChannel.socket().getSendBufferSize(), socketChannel.socket().getReceiveBufferSize()));

                            socketChannel.configureBlocking(false);
                            socketChannel.socket().setSendBufferSize(bufferSize);
                            socketChannel.socket().setReceiveBufferSize(bufferSize);
                            socketChannel.socket().setTcpNoDelay(true);
                            // socketChannel.socket().setSoLinger(false);
                            // socketChannel.socket().setReuseAddress(true)

                            final RequestData requestData = new RequestData(postPayLoad);
                            requestData.socketChannel = socketChannel;

                            requestsPendingToSend.offer(requestData);

                            final int selectorIndex = random.nextInt(numOfCpus);
                            if (logger.isDebugEnabled())
                                logger.debug("randomly selected: {}", selectorIndex);
                            final Selector selector = selectors[selectorIndex];
                            selector.wakeup();
                        }

                        requestsScheduled.addAndGet(requestsToSend);
                        lastTickWorkTookMs.setValue(System.currentTimeMillis() - now);
                    }
                } catch (Exception e) {
                    logger.error("error partitioning data", e);
                    // it's all over let's unblock ourselves

                    while (allDone.getCount() > 0)
                        allDone.countDown();
                }
            }
        });


        if (logger.isInfoEnabled()) {
            final SocketChannel tmpSocketChannel = SocketChannel.open();
            logger.info(String.format("default send/recv buffer sizes: %d %d", tmpSocketChannel.socket().getSendBufferSize(), tmpSocketChannel.socket().getReceiveBufferSize()));
            logger.info(String.format("current send/recv buffer sizes: %d %d", bufferSize, bufferSize));
            logger.info(String.format("default tcpNoDelay: %s", tmpSocketChannel.socket().getTcpNoDelay()));
            logger.info(String.format("current tcpNoDelay: %s", true));

            tmpSocketChannel.close();
        }

        logger.info("Legend:\n" +
                "es\t\t- elapsed seconds\n" +
                "rsd\t\t- requests scheduled per tick\n" +
                "irps\t- initiated requests per tick\n" +
                "rps\t\t- processed requests per tick\n" +
                "cer\t\t- currently executed requests\n" +
                "err\t\t- errors per tick\n" +
                "rpts\t- requests pending to send\n" +
                "ost\t\t- max time SocketChannel#open took in ms\n" +
                "cnd\t\t- total requests ever connected\n" +
                "wrn\t\t- total requests ever written\n" +
                "rd\t\t- total requests ever got response");

        final long startTs = System.currentTimeMillis();
        long lastRequestsScheduled = requestsScheduled.get();
        int lastInitiatedReqCount = initiatedRequestsCount.get();
        int lastProcessedReqCount = processedRequestsCount.get();
        int lastErrorCount = errorCount.get();

        while (!allDone.await(1, TimeUnit.SECONDS)) {
            final int newRequestsScheduled = requestsScheduled.get();
            final int newInitiatedReqCount = initiatedRequestsCount.get();
            final int newProcessedReqCount = processedRequestsCount.get();
            final int newErrorCount = errorCount.get();
            final long now = System.currentTimeMillis();
            final int elapsedSec = Ints.checkedCast(TimeUnit.MILLISECONDS.toSeconds(now - startTs));

            logger.info(String.format("es: %3d, rsd: %4d, irps: %4d, rps: %4d, cer: %5d, err: %5d, rpts: %4d, ost: %4d, cnd %5d, wrn %5d, rd %5d",
                    elapsedSec,
                    (newRequestsScheduled - lastRequestsScheduled),
                    (newInitiatedReqCount - lastInitiatedReqCount),
                    (newProcessedReqCount - lastProcessedReqCount),
                    currentlyExecutingRequests.size(),
                    (newErrorCount - lastErrorCount),
                    requestsPendingToSend.size(),
                    openSocketTookMsMax.getAndSet(0),
                    connectedCount.get(),
                    writtenCount.get(),
                    readCount.get()));
            lastRequestsScheduled = newRequestsScheduled;
            lastInitiatedReqCount = newInitiatedReqCount;
            lastProcessedReqCount = newProcessedReqCount;
            lastErrorCount = newErrorCount;
        }

        final int errorCountValue = errorCount.get();
        if (errorCountValue > 0)
            logger.error("total number of errors: {}", errorCountValue);
    }

    private static class RequestData {
        public ByteBuffer payloadReadBuffer = ByteBuffer.allocate(bufferSize);
        public ByteBuffer responseReadBuffer = ByteBuffer.allocate(bufferSize);
        public CharBuffer currentHeaderCharBuffer = CharBuffer.allocate(bufferSize);
        public SocketChannel socketChannel;
        public ByteBuffer payloadWriteBuffer;
        public AtomicInteger readBytes = new AtomicInteger();
        public CharsetDecoder decoder = encoding.newDecoder().onMalformedInput(CodingErrorAction.REPLACE).onUnmappableCharacter(CodingErrorAction.REPLACE);
        private ByteBuffer lineSeparatorBuffer = encoding.encode(lineSeparatorCharBuffer.asReadOnlyBuffer());
        boolean inProcessingHeader = true;

        private RequestData(byte[] payload) {
            payloadWriteBuffer = ByteBuffer.wrap(payload);
        }

        public void processReadBuffer() {
            if (!inProcessingHeader) {
                responseReadBuffer.flip();
                payloadReadBuffer.put(responseReadBuffer);
                responseReadBuffer.clear();
            }

            responseReadBuffer.flip();
            int mark = 0;

            while (responseReadBuffer.hasRemaining()) {
                final byte currentByte = responseReadBuffer.get();
                if (currentByte == lineSeparatorBuffer.get(lineSeparatorBuffer.position())) {
                    lineSeparatorBuffer.get(); // advance pointer in the lineseparator buffer.
                    if (!lineSeparatorBuffer.hasRemaining()) {
                        lineSeparatorBuffer.flip();

                        final int limitToRestore = responseReadBuffer.limit();
                        try {
                            responseReadBuffer.limit(responseReadBuffer.position());
                            responseReadBuffer.position(mark);
                            decoder.decode(responseReadBuffer, currentHeaderCharBuffer, true);
                            mark = responseReadBuffer.position();
                        } finally {
                            responseReadBuffer.limit(limitToRestore);
                        }

                        currentHeaderCharBuffer.flip();
                        
                        // analyse the header if it's empty string
                        if (currentHeaderCharBuffer.equals(lineSeparatorCharBuffer)) {
                            inProcessingHeader = false;
                            payloadReadBuffer.put(responseReadBuffer);
                            responseReadBuffer.clear();
                            return;
                        }
                        
                        currentHeaderCharBuffer.clear();
                    }
                }
            }

            responseReadBuffer.position(mark);
            if (responseReadBuffer.hasRemaining())
                decoder.decode(responseReadBuffer, currentHeaderCharBuffer, false);
            responseReadBuffer.clear();
        }

        public void finishRead() throws IOException {
            payloadReadBuffer.flip();
            payloadWriteBuffer.flip();

            if (!payloadReadBuffer.equals(payloadWriteBuffer)) {
                if (verboseErrors)
                    logger.error("unexpected response: " + new String(payloadReadBuffer.array(), encoding));

                throw new IOException("possible connection reset");
            }
        }
    }

    private static boolean setIfGreater(AtomicLong container, long value) {
        while (true) {
            long current = container.get();
            if (value <= current)
                return false;
            if (container.compareAndSet(current, value))
                return true;
        }
    }

    private static class TimeCallback {
        public long scheduledAt;
        public Runnable callback;
    }
}
