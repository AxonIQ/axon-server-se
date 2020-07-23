package io.axoniq.axonserver.cluster.util;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.UnknownFieldSet;

/**
 * @author Marc Gathier
 */
public class GrpcSignedLongUtils {

    public static Long getSignedLongField(GeneratedMessageV3 failure, int fieldNumber) {
        if (!failure.getUnknownFields().hasField(fieldNumber)) {
            return 0L;
        }
        long rawValue = failure.getUnknownFields().getField(fieldNumber).getVarintList().get(0);
        return CodedInputStream.decodeZigZag64(rawValue);
    }

    public static UnknownFieldSet.Field createSignedIntField(long signedValue) {
        return UnknownFieldSet.Field.newBuilder()
                                    .addVarint(CodedOutputStream.encodeZigZag64(signedValue))
                                    .build();
    }
}
