package dev.maxipool.demowebfluxwebsockethtmx;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.tools.agent.ReactorDebugAgent;

@SpringBootApplication
public class DemoWebfluxWebsocketHtmxApplication {

    public static void main(String[] args) {
      ReactorDebugAgent.init();
        SpringApplication.run(DemoWebfluxWebsocketHtmxApplication.class, args);
    }

}
