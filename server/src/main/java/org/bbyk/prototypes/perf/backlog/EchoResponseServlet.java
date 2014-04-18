package org.bbyk.prototypes.perf.backlog;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * @author bbyk
 */
public class EchoResponseServlet extends HttpServlet {
    private final static Charset utf8Charset = Charset.forName("utf8");

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            final long sleepTimeMs = Long.parseLong(req.getParameter("sleepTimeMs"));
            if (sleepTimeMs > 0)
                Thread.sleep(sleepTimeMs);
            final String content = req.getParameter("echoContent");
            final byte[] contentBytes = content.getBytes(utf8Charset);
            resp.setContentLength(contentBytes.length);
            resp.getOutputStream().write(contentBytes);
            resp.getOutputStream().flush();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
