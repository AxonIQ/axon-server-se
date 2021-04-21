package io.axoniq.axonserver.refactoring.transport.grpc;

import io.axoniq.axonserver.grpc.ErrorMessage;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.refactoring.messaging.api.SerializedObject;
import io.axoniq.axonserver.refactoring.messaging.command.SerializedCommandResponse;
import io.axoniq.axonserver.refactoring.messaging.command.api.CommandResponse;
import io.axoniq.axonserver.refactoring.transport.Mapper;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @author Milan Savic
 */
@Component("commandResponseMapper")
public class CommandResponseMapper implements Mapper<CommandResponse, SerializedCommandResponse> {

    private final Mapper<SerializedObject, io.axoniq.axonserver.grpc.SerializedObject> serializedObjectMapper;
    private final Mapper<Map<String, Object>, Map<String, MetaDataValue>> metadataMapper;

    public CommandResponseMapper(
            Mapper<SerializedObject, io.axoniq.axonserver.grpc.SerializedObject> serializedObjectMapper,
            Mapper<Map<String, Object>, Map<String, MetaDataValue>> metadataMapper) {
        this.serializedObjectMapper = serializedObjectMapper;
        this.metadataMapper = metadataMapper;
    }

    @Override
    public SerializedCommandResponse map(CommandResponse origin) {
        if (origin instanceof GrpcCommandResponse) {
            return ((GrpcCommandResponse) origin).serializedCommandResponse();
        }
        io.axoniq.axonserver.grpc.command.CommandResponse.Builder builder = io.axoniq.axonserver.grpc.command.CommandResponse
                .newBuilder()
                .setMessageIdentifier(origin.message().id())
                .putAllMetaData(metadataMapper.map(origin.message().metadata()));
        origin.error().ifPresent(e -> builder.setErrorCode(e.code())
                                             .setErrorMessage(ErrorMessage.newBuilder()
                                                                          .setErrorCode(e.code())
                                                                          .setMessage(e.message())
                                                                          .setLocation(e.source())
                                                                          .addAllDetails(e.details())));
        origin.message().payload().ifPresent(p -> builder.setPayload(serializedObjectMapper.map(p)));
        return new SerializedCommandResponse(builder.build());
    }

    @Override
    public CommandResponse unmap(SerializedCommandResponse origin) {
        return new GrpcCommandResponse(serializedObjectMapper, origin);
    }
}
