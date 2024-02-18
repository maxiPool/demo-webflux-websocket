package dev.maxipool.demowebfluxwebsockethtmx.websocket.model;

import lombok.Builder;
import lombok.With;

@Builder
@With
public record UpdateRecordMessage(String id, String field1, String field2) {
}
