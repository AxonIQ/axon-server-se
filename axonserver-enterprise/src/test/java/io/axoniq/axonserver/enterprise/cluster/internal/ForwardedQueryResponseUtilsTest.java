package io.axoniq.axonserver.enterprise.cluster.internal;

import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.grpc.MetaDataValue;
import io.axoniq.axonserver.grpc.ProcessingInstruction;
import io.axoniq.axonserver.grpc.SerializedObject;
import io.axoniq.axonserver.grpc.internal.ForwardedQueryResponse;
import io.axoniq.axonserver.grpc.query.QueryResponse;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ForwardedQueryResponseUtilsTest {

    @Test
    public void testUnwrappingForwardedQueryResponseToQueryResponse() {
        SerializedObject serializedObject = SerializedObject.getDefaultInstance();
        ForwardedQueryResponse forwardedResponse = ForwardedQueryResponse.newBuilder()
                                                                         .setRespondingClientId("clientId")
                                                                         .setPayload(serializedObject)
                                                                         .putMetaData("meta", MetaDataValue.newBuilder().setTextValue("value").build())
                                                                         .putMetaData("meta2", MetaDataValue.newBuilder().setTextValue("value2").build())
                                                                         .setMessageIdentifier("messageId")
                                                                         .setRequestIdentifier("requestId")
                                                                         .build();

        QueryResponse result = ForwardedQueryResponseUtils.unwrap(forwardedResponse);
        assertEquals("requestId", result.getRequestIdentifier());
        assertEquals("value", result.getMetaDataMap().get("meta").getTextValue());
        assertEquals("value2", result.getMetaDataMap().get("meta2").getTextValue());
    }

    @Test
    public void testClientIdRetrievedFromForwardedResponseIfPresent() {
        ForwardedQueryResponse forwardedResponse = ForwardedQueryResponse.newBuilder()
                                                                         .setRespondingClientId("clientId")
                                                                         .addProcessingInstructions(clientIdInstruction("wrong"))
                                                                         .build();

        assertEquals("clientId", ForwardedQueryResponseUtils.getDispatchingClient(forwardedResponse));
    }

    @Test
    public void testClientIdRetrievedFromProcessingInstructionForLegacyQueryResponseMessage() throws InvalidProtocolBufferException {
        QueryResponse response = QueryResponse.newBuilder()
                                              .addProcessingInstructions(clientIdInstruction("clientId"))
                                              .build();

        // this ensures actual behavior of gRPC's forward and backward compatibility features
        ForwardedQueryResponse forwardedResponse = ForwardedQueryResponse.parseFrom(response.toByteArray());


        assertEquals("clientId", ForwardedQueryResponseUtils.getDispatchingClient(forwardedResponse));
    }

    @NotNull
    private ProcessingInstruction.Builder clientIdInstruction(String clientId) {
        return ProcessingInstruction.newBuilder().setKeyValue(5).setValue(MetaDataValue.newBuilder().setTextValue(clientId));
    }

    @Test
    public void testWrappingQueryResponseToForwardedQueryResponse() {
        SerializedObject serializedObject = SerializedObject.getDefaultInstance();
        QueryResponse queryResponse = QueryResponse.newBuilder()
                                                   .setPayload(serializedObject)
                                                   .putMetaData("meta", MetaDataValue.newBuilder().setTextValue("value").build())
                                                   .putMetaData("meta2", MetaDataValue.newBuilder().setTextValue("value2").build())
                                                   .setMessageIdentifier("messageId")
                                                   .setRequestIdentifier("requestId")
                                                   .build();

        ForwardedQueryResponse result = ForwardedQueryResponseUtils.wrapForForwarding(queryResponse, "testClient");
        assertEquals("requestId", result.getRequestIdentifier());
        assertEquals("value", result.getMetaDataMap().get("meta").getTextValue());
        assertEquals("value2", result.getMetaDataMap().get("meta2").getTextValue());
        assertEquals("testClient", result.getRespondingClientId());
        assertEquals("Expected ProcessingInstruction 5 for backwards compatibility",
                     "testClient",
                     result.getProcessingInstructionsList().stream().filter(p -> p.getKeyValue() == 5)
                           .map(p -> p.getValue().getTextValue()).findFirst().orElse(""));
    }

    /*
    This test case validates some assumptions made on the structure of gRPC message formats. Specifically, all
    fields defined in QueryResponse must have an identical counterpart in the ForwardedQueryResponse. The other way
    round is not the case: ForwardedQueryResponse may define fields for which no counterpart exists in QueryResponse.
    However, if one does exist, then the type must be identical.
     */
    @Test
    public void testForwardedQueryResponseIsCompatibleWithQueryResponse() {
        Descriptors.Descriptor fqrFields = ForwardedQueryResponse.getDescriptor();
        Descriptors.Descriptor qrFields = QueryResponse.getDescriptor();

        // for each QueryResponseField, we expect a forwarded counterpart with the same ID, type and name
        qrFields.getFields().forEach(f -> {
            assertNotNull("No field with number " + f.getNumber() + " could be found in ForwardedQueryResponse", fqrFields.findFieldByNumber(f.getNumber()));
            assertEquals("Field id " + f.getNumber() + " does not have the same type in ForwardedQueryResponse and QueryResponse ",
                         f.getType(), fqrFields.findFieldByNumber(f.getNumber()).getType());
            assertEquals("Field id " + f.getNumber() + " does not have the same name in ForwardedQueryResponse and QueryResponse ",
                         f.getName(), fqrFields.findFieldByNumber(f.getNumber()).getName());
        });

        // for each ForwardedQueryResponseField also defined in QueryResponseField, we expect the same type
        fqrFields.getFields().forEach(f -> {
            Descriptors.FieldDescriptor qrField = qrFields.findFieldByNumber(f.getNumber());
            if (qrField != null) {
                assertEquals(f.getType(), fqrFields.findFieldByNumber(f.getNumber()).getType());
            }
        });
    }


}
