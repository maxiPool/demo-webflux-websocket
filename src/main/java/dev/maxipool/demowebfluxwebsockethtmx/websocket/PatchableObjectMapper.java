package dev.maxipool.demowebfluxwebsockethtmx.websocket;

import dev.maxipool.demowebfluxwebsockethtmx.websocket.model.MyPatchableObject;
import dev.maxipool.demowebfluxwebsockethtmx.websocket.model.UpdateRecordMessage;
import org.mapstruct.BeanMapping;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingTarget;

import static org.mapstruct.NullValuePropertyMappingStrategy.IGNORE;
import static org.mapstruct.ReportingPolicy.ERROR;

@Mapper(unmappedTargetPolicy = ERROR)
public interface PatchableObjectMapper {

  @Mapping(target = "id", ignore = true)
  @BeanMapping(nullValuePropertyMappingStrategy = IGNORE)
  void partialUpdate(UpdateRecordMessage patch, @MappingTarget MyPatchableObject myPatchableObject);

}
