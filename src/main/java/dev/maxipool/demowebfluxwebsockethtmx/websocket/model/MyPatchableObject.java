package dev.maxipool.demowebfluxwebsockethtmx.websocket.model;

import lombok.*;

@NoArgsConstructor
@Getter
@Setter
@ToString
@EqualsAndHashCode(of = "id")
public class MyPatchableObject {

  private String id;
  private String field1;
  private String field2;

}
