package org.bbyk.prototypes.perf.connector;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.ReadListener;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author bbyk
 */
@WebServlet(asyncSupported=true)
public class AsyncEchoResponseServlet extends HttpServlet {
    private final static Logger logger = LoggerFactory.getLogger(AsyncEchoResponseServlet.class);

    @Override
    protected void doPost(HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        // http://stackoverflow.com/a/7850284
        req.setAttribute("org.apache.catalina.ASYNC_SUPPORTED", true);
        
        final AsyncContext context = req.startAsync();
        context.setTimeout(1000000);
        final ServletInputStream inputStream = req.getInputStream();
        final ByteBuffer readBuffer = ByteBuffer.allocate(16 * 1024);
        final byte[] buffer = new byte[1024];

        final ServletOutputStream outputStream = resp.getOutputStream();
        context.addListener(new AsyncListener() {
            @Override
            public void onComplete(AsyncEvent event) throws IOException {
                if (logger.isDebugEnabled())
                    logger.debug("read/write completed");
            }

            @Override
            public void onTimeout(AsyncEvent event) throws IOException {
            }

            @Override
            public void onError(AsyncEvent event) throws IOException {
                logger.error("error", event.getThrowable());
            }

            @Override
            public void onStartAsync(AsyncEvent event) throws IOException {

            }
        });
        
        inputStream.setReadListener(new ReadListener() {
            @Override
            public void onDataAvailable() throws IOException {
                int len;
                while (inputStream.isReady() && (len = inputStream.read(buffer)) != -1)
                {
                    readBuffer.put(buffer, 0, len);
                }
            }

            @Override
            public void onAllDataRead() throws IOException {
                readBuffer.flip();
                context.getResponse().setContentLength(readBuffer.limit());
                outputStream.setWriteListener(new WriteListener() {
                    @Override
                    public void onWritePossible() throws IOException {
                        while (outputStream.isReady() && readBuffer.hasRemaining()) {
                            outputStream.write(readBuffer.get());
                        }

                        if (!readBuffer.hasRemaining())
                            context.complete();
                    }

                    @Override
                    public void onError(Throwable t) {
                        logger.error("error", t);
                        context.complete();
                    }
                });
            }

            @Override
            public void onError(Throwable t) {
                logger.error("error", t);
                context.complete();
            }
        });
        
    }
}
