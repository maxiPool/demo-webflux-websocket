package dev.maxipool.demowebfluxwebsockethtmx.websocket;

import dev.maxipool.demowebfluxwebsockethtmx.websocket.model.MyPatchableObject;
import dev.maxipool.demowebfluxwebsockethtmx.websocket.model.UpdateRecordMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
@RequiredArgsConstructor
public class PatchableRecordsController {

  private final PatchableObjectMapper patchableObjectMapper;

  private static final Map<String, MyPatchableObject> records = new HashMap<>();

  static {
    for (int i = 0; i < 10; i++) {
      var e = new MyPatchableObject();
      e.setId(String.valueOf(i));
      e.setField1("f1-" + i);
      e.setField2("f2-" + i);
      records.put(e.getId(), e);
    }
  }

  /**
   * Websocket endpoint to update records
   * Fine for a small number of small records (e.g. 5_000x 3x small string fields)
   */
  @MessageMapping("/update-record")
  @SendTo("/topic/all-records")
  public List<MyPatchableObject> patchMethod(UpdateRecordMessage updateMessage) {
    var myPatchableObject = records.get(updateMessage.id());
    patchableObjectMapper.partialUpdate(updateMessage, myPatchableObject);
    return new ArrayList<>(records.values());
  }

  /**
   * WebSocket endpoint to get the list of all records
   * Fine for a small number of small records (e.g. 5_000x 3x small string fields)
   */
  @MessageMapping("/get-all-records")
  @SendTo("/topic/all-records")
  public List<MyPatchableObject> getAllRecords() {
    return new ArrayList<>(records.values());
  }

  @RestController
  @Slf4j
  @RequestMapping("api/v1")
  public static class MyRestController {
    @GetMapping("all-records")
    public List<MyPatchableObject> getAllRecords() {
      log.info("REST API: getAllRecords");
      return new ArrayList<>(records.values());
    }
  }

}
