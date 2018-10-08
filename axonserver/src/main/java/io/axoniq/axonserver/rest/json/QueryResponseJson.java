package io.axoniq.axonserver.rest.json;

import io.axoniq.axonserver.grpc.query.QueryResponse;
import io.axoniq.platform.KeepNames;

/**
 * Author: marc
 */
@KeepNames
public class QueryResponseJson {
    private final String messageIdentifier;
    private final String requestIdentifier;
    private final String errorCode;
    private final MessageJson message;
    private final SerializedObjectJson payload;
    private final MetaDataJson metaData;

    public QueryResponseJson(QueryResponse r) {
        messageIdentifier = r.getMessageIdentifier();
        requestIdentifier = r.getRequestIdentifier();
        errorCode = r.getErrorCode();
        payload = r.hasPayload() ? new SerializedObjectJson(r.getPayload()) : null;
        message = r.hasMessage() ? new MessageJson(r.getMessage()) : null;
        metaData = new MetaDataJson(r.getMetaDataMap());
    }

    public String getMessageIdentifier() {
        return messageIdentifier;
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

    public MetaDataJson getMetaData() {
        return metaData;
    }
}
