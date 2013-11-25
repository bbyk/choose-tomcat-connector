package org.bbyk.examples.spring.properties.imports;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * @author bbyk
 */
@Controller
public class StaticResponseController {
    private String injectedResponse;
    private int sleepTimeMs;

    public void setSleepTimeMs(int sleepTimeMs) {
        this.sleepTimeMs = sleepTimeMs;
    }

    public void setInjectedResponse(String value) {
        this.injectedResponse = value;
    }

    @RequestMapping
    @ResponseBody
    public String execute(HttpServletRequest request, HttpServletResponse response) throws Exception {
        if (sleepTimeMs > 0)
            Thread.sleep(sleepTimeMs);
        return injectedResponse;
    }
}
