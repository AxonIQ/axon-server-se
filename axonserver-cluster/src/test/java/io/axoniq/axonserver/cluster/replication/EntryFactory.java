package io.axoniq.axonserver.cluster.replication;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;

/**
 * @author Marc Gathier
 */
public class EntryFactory {

    public static Entry newEntry(long term, long index) {
        SerializedObject serializedObject = SerializedObject.newBuilder().setData(ByteString.copyFromUtf8(
                "Test: " + index))
                                                            .setType("Test")
                                                            .build();
        return Entry.newBuilder()
                    .setTerm(term)
                    .setIndex(index)
                    .setSerializedObject(serializedObject)
                    .build();
    }
}
