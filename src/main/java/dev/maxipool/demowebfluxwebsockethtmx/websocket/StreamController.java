package dev.maxipool.demowebfluxwebsockethtmx.websocket;

import dev.maxipool.demowebfluxwebsockethtmx.websocket.model.Greeting;
import dev.maxipool.demowebfluxwebsockethtmx.websocket.model.HelloMessage;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Controller;
import org.springframework.web.util.HtmlUtils;

@Controller
public class StreamController {

  @MessageMapping("/hello")
  @SendTo("/topic/greetings")
  public Greeting greeting(HelloMessage message) throws Exception {
    Thread.sleep(1000); // simulated delay
    return new Greeting("Hello, " + HtmlUtils.htmlEscape(message.name()) + "!");
  }

}
