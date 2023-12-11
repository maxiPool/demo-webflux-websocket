package dev.maxipool.demowebfluxwebsockethtmx.websocket;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.SimpleAsyncTaskScheduler;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

/**
 * <a href="https://docs.spring.io/spring-framework/reference/web/websocket/stomp/enable.html">--> Spring Websockets Documentation here <--</a>
 */
@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

  private final TaskScheduler scheduler = new SimpleAsyncTaskScheduler();

  @Override
  public void configureMessageBroker(MessageBrokerRegistry config) {
    config.enableSimpleBroker("/topic")
        .setHeartbeatValue(new long[]{0, 120_000})
        .setTaskScheduler(scheduler);
    config.setApplicationDestinationPrefixes("/app");
  }

  @Override
  public void registerStompEndpoints(StompEndpointRegistry registry) {
    registry.addEndpoint("/handshake");
    registry.addEndpoint("/handshake").withSockJS();
  }

}
