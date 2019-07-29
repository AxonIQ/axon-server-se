package io.axoniq.axonserver.enterprise.cluster.internal;

import com.google.protobuf.Descriptors;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.ProcessingInstruction;
import io.axoniq.axonserver.grpc.internal.ForwardedQueryResponse;
import io.axoniq.axonserver.grpc.query.QueryResponse;

import java.util.Map;

/**
 * Utility class to convert between QueryResponse and ForwardedQueryResponse messages.
 *
 * @author Allard Buijze
 * @since 4.2
 */
public class ForwardedQueryResponseUtils {

    private static final int DEPRECATED_TARGET_CLIENT_KEY = 5;

    private ForwardedQueryResponseUtils() {

    }

    /**
     * Builds a ForwardedQueryResponse using the given {@code queryResponse} and {@code respondingClientId}. The
     * resulting ForwardedQueryResponse may be used to forward responses to other nodes in the cluster, if the node
     * receiving the {@code queryResponse} is not directly connected to its destination.
     *
     * @param queryResponse      the response that needs to be forwarded across the cluster
     * @param respondingClientId the client ID of the component that sent the original response
     * @return The builder of the message with QueryResponse related fields initialized
     */
    public static ForwardedQueryResponse wrapForForwarding(QueryResponse queryResponse, String respondingClientId) {
        // we expect to be able to put all QueryResponse fields directly into a ForwardedResponse
        ForwardedQueryResponse.Builder builder = ForwardedQueryResponse.newBuilder();
        Descriptors.Descriptor outputFields = ForwardedQueryResponse.getDescriptor();
        Map<Descriptors.FieldDescriptor, Object> inputValues = queryResponse.getAllFields();
        queryResponse.getDescriptorForType().getFields().forEach(inputField -> {
            Descriptors.FieldDescriptor outputField = outputFields.findFieldByNumber(inputField.getNumber());
            Object value = inputValues.get(inputField);
            if (value != null) {
                builder.setField(outputField, value);
            }
        });

        // for backwards compatibility, ProcessingInstruction keyID 5 (which used to contain the clientID) is added
        builder.addProcessingInstructions(ProcessingInstruction.newBuilder()
                                                               .setKeyValue(DEPRECATED_TARGET_CLIENT_KEY)
                                                               .setValue(MetaDataValue.newBuilder()
                                                                                      .setTextValue(respondingClientId)).build());

        return builder.setRespondingClientId(respondingClientId).build();
    }

    /**
     * Unwrap the given {@code forwardedResponse} to reconstruct the QueryResponse object that may be sent to the final
     * destination.
     *
     * @param forwardedResponse The message received on internal channels containing the data for the QueryResponse
     * @return the QueryResponse message to send to the querying component
     */
    public static QueryResponse unwrap(ForwardedQueryResponse forwardedResponse) {
        Map<Descriptors.FieldDescriptor, Object> inputValues = forwardedResponse.getAllFields();
        QueryResponse.Builder responseBuilder = QueryResponse.newBuilder();
        for (int i = 1; i <= 7; i++) {
            Descriptors.FieldDescriptor outputField = QueryResponse.getDescriptor().findFieldByNumber(i);
            Descriptors.FieldDescriptor inputField = ForwardedQueryResponse.getDescriptor().findFieldByNumber(i);
            Object value = inputValues.get(inputField);
            if (value != null) {
                responseBuilder.setField(outputField, value);
            }
        }

        return responseBuilder.build();

    }

    /**
     * Extracts the ClientID of the component that originally sent the response from the given
     * {@code forwardedResponse}.
     * <p>
     * It takes into account that older versions (< 4.1) sent the Client ID as ProcessingInstruction with key
     * 5. Given gRPC's backward and forward compatibility support, the clientId in the responseMessage will be an empty
     * string.
     *
     * @param forwardedResponse The response containing the ClientID of the responding client
     * @return The clientID of the responding client
     */
    public static String getDispatchingClient(ForwardedQueryResponse forwardedResponse) {
        String clientId = forwardedResponse.getRespondingClientId();
        if ("".equals(clientId)) {
            // this may be a legacy component and it may have added the client ID as processing instruction
            return forwardedResponse.getProcessingInstructionsList()
                                    .stream()
                                    .filter(i -> i.getKeyValue() == DEPRECATED_TARGET_CLIENT_KEY)
                                    .map(i -> i.getValue().getTextValue())
                                    .findFirst()
                                    .orElse("");
        }
        return clientId;
    }

}
