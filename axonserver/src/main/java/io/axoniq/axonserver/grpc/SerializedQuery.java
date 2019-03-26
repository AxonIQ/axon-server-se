package io.axoniq.axonserver.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.grpc.query.QueryRequest;

/**
 * Wrapper around {@link QueryRequest} to reduce serialization/deserialization.
 * @author Marc Gathier
 */
public class SerializedQuery  {

    private final String client;
    private final String context;
    private volatile QueryRequest query;
    private final byte[] serializedData;

    public SerializedQuery(String context, QueryRequest query) {
        this(context, null, query);
    }

    public SerializedQuery(String context, String client, QueryRequest query) {
        this.context = context;
        this.client = client;
        this.serializedData = query.toByteArray();
        this.query = query;
    }

    public SerializedQuery(String context, String client, byte[] readByteArray) {
        this.context = context;
        this.client = client;
        serializedData = readByteArray;
    }


    public QueryRequest query() {
        if( query == null) {
            try {
                query = QueryRequest.parseFrom(serializedData);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
        return query;
    }

    public String context() {
        return context;
    }

    public String client() {
        return client;
    }

    public ByteString toByteString() {
        return ByteString.copyFrom(serializedData);
    }

    public SerializedQuery withTimeout(long remainingTime) {
        int timeoutIndex = -1;
        QueryRequest request = query();
        for(int i = 0; i < request.getProcessingInstructionsList().size(); i++) {
            if(ProcessingKey.TIMEOUT.equals(request.getProcessingInstructions(i).getKey()) ) {
                timeoutIndex = i;
                break;
            }
        }


        if( timeoutIndex >= 0) {
            request = QueryRequest.newBuilder(request)
                    .removeProcessingInstructions(timeoutIndex)
                    .addProcessingInstructions(ProcessingInstructionHelper.timeout(remainingTime))
                    .build();
        } else {
            request = QueryRequest.newBuilder(request)
                    .addProcessingInstructions(ProcessingInstructionHelper.timeout(remainingTime))
                    .build();
        }
        return new SerializedQuery(context, client, request);

    }

    public String getMessageIdentifier() {
        return query().getMessageIdentifier();
    }
}
