package dev.maxipool.demowebfluxwebsockethtmx.websocket;

import dev.maxipool.demowebfluxwebsockethtmx.websocket.model.Greeting;
import dev.maxipool.demowebfluxwebsockethtmx.websocket.model.HelloMessage;
import dev.maxipool.demowebfluxwebsockethtmx.websocket.model.MyPatchableObject;
import dev.maxipool.demowebfluxwebsockethtmx.websocket.model.UpdateRecordMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Controller;
import org.springframework.web.util.HtmlUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
@RequiredArgsConstructor
public class StreamController {

  @MessageMapping("/hello")
  @SendTo("/topic/greetings")
  public Greeting greeting(HelloMessage message) throws Exception {
    Thread.sleep(1000); // simulated delay
    return new Greeting("Hello, " + HtmlUtils.htmlEscape(message.name()) + "!");
  }

  private final PatchableObjectMapper patchableObjectMapper;

  private static final Map<String, MyPatchableObject> records = new HashMap<>();

  static {
    var e1 = new MyPatchableObject();
    var e2 = new MyPatchableObject();
    e1.setId("1");
    e1.setField1("e1-f1");
    e1.setField2("e1-f2");
    e2.setId("2");
    e2.setField1("e2-f1");
    e2.setField2("e2-f2");
    records.put(e1.getId(), e1);
    records.put(e2.getId(), e2);
  }

  /**
   * Websocket endpoint to update records
   */
  @MessageMapping("/updateRecord")
  @SendTo("/topic/allRecords")
  public List<MyPatchableObject> patchMethod(UpdateRecordMessage updateMessage) {
    var myPatchableObject = records.get(updateMessage.id());
    patchableObjectMapper.partialUpdate(updateMessage, myPatchableObject);
    return new ArrayList<>(records.values());
  }

  /**
   * WebSocket endpoint to get the list of all records
   */
  @MessageMapping("/getAllRecords")
  @SendTo("/topic/allRecords")
  public List<MyPatchableObject> getAllRecords() {
    return new ArrayList<>(records.values());
  }

}
