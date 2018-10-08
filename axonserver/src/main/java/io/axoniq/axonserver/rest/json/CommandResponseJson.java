package io.axoniq.axonserver.rest.json;

import io.axoniq.axonserver.grpc.command.CommandResponse;
import io.axoniq.platform.KeepNames;

/**
 * Author: marc
 */
@KeepNames
public class CommandResponseJson {

    private final String requestIdentifier;
    private final String errorCode;
    private final MessageJson message;
    private final SerializedObjectJson payload;
    private final String messageIdentifier;
    private MetaDataJson metaData;

    public CommandResponseJson(CommandResponse r) {
        requestIdentifier = r.getRequestIdentifier();
        errorCode = r.getErrorCode();
        message = r.hasMessage() ? new MessageJson(r.getMessage()) : null;
        payload = r.hasPayload() ? new SerializedObjectJson(r.getPayload()) : null;
        messageIdentifier = r.getMessageIdentifier();
        metaData = new MetaDataJson(r.getMetaDataMap());
    }

    public String getRequestIdentifier() {
        return requestIdentifier;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public MessageJson getMessage() {
        return message;
    }

    public SerializedObjectJson getPayload() {
        return payload;
    }

    public String getMessageIdentifier() {
        return messageIdentifier;
    }

    public MetaDataJson getMetaData() {
        return metaData;
    }
}
