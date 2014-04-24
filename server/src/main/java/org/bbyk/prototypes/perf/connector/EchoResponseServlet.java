package org.bbyk.prototypes.perf.connector;

import org.apache.commons.io.IOUtils;

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
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        final byte[] content = IOUtils.toByteArray(req.getInputStream());
       
        resp.setContentLength(content.length);
        resp.getOutputStream().write(content);
        resp.getOutputStream().flush();
    }
}
