package io.axoniq.axonserver.grpc;

import com.google.protobuf.ByteString;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import io.axoniq.axonserver.localstorage.SerializedEvent;
import io.axoniq.axonserver.localstorage.SerializedTransactionWithToken;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marc Gathier
 */
public class SerializedTransactionWithTokenConverter {

    public static SerializedTransactionWithToken asSerializedTransactionWithToken(
            TransactionWithToken transactionWithToken) {
        List<SerializedEvent> serializedEvents = transactionWithToken.getEventsList().stream()
                                                                     .map(bytes -> new SerializedEvent(bytes.toByteArray()))
                                                                     .collect(
                                                                             Collectors.toList());

        return new SerializedTransactionWithToken(transactionWithToken.getToken(),
                                                  (byte)transactionWithToken.getVersion(),
                                                  serializedEvents);
    }


    public static ByteString asByteString(SerializedTransactionWithToken transactionWithToken) {
        List<ByteString> events = transactionWithToken.getEvents().stream().map(s -> s.asByteString()).collect(
                Collectors.toList());
        return TransactionWithToken.newBuilder().setVersion(transactionWithToken.getVersion())
                .setToken(transactionWithToken.getToken())
                .addAllEvents(events)
                .build().toByteString();

    }
}
