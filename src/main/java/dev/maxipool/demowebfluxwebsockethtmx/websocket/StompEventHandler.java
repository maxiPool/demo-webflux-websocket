package dev.maxipool.demowebfluxwebsockethtmx.websocket;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.messaging.SessionSubscribeEvent;

import static org.springframework.messaging.simp.stomp.StompHeaderAccessor.wrap;

@Slf4j
@Component
public class StompEventHandler {

  @EventListener
  public void on(SessionSubscribeEvent event) {
    var accessor = wrap(event.getMessage());

    log.info("Subscribe received for: {}", event.getMessage());
    log.info("Destination: {}", accessor.getDestination());
  }

}
