package io.axoniq.axonserver.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.grpc.command.Command;

/**
 * Wrapper around gRPC command to reduce serialization/deserialization.
 * @author Marc Gathier
 */
public class SerializedCommand  {

    private volatile String client;
    private volatile String messageId;
    private volatile Command command;
    private final byte[] serializedData;

    public SerializedCommand(Command command) {
        this.serializedData = command.toByteArray();
        this.command = command;
    }

    public SerializedCommand(byte[] readByteArray) {
        serializedData = readByteArray;
    }

    public SerializedCommand(byte[] toByteArray, String client, String messageId) {
        this(toByteArray);
        this.client = client;
        this.messageId = messageId;
    }

    public static SerializedCommand getDefaultInstance() {
        return new SerializedCommand(Command.getDefaultInstance());
    }


    public int getSerializedSize() {
        return serializedData.length;
    }

    public byte[] toByteArray() {
        return serializedData;
    }

    public Command wrapped() {
        if( command == null) {
            try {
                command = Command.parseFrom(serializedData);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
        return command;
    }


    public ByteString toByteString() {
        return ByteString.copyFrom(serializedData);
    }

    public String getMessageIdentifier() {
        if( messageId != null) return messageId;
        return wrapped().getMessageIdentifier();
    }

    public String getName() {
        return wrapped().getName();
    }

    public String getClient() {
        return client;
    }

    public String getCommand() {
        return wrapped().getName();
    }

    public String getRoutingKey() {
        return ProcessingInstructionHelper.routingKey(wrapped().getProcessingInstructionsList());
    }

    public long getPriority() {
        return ProcessingInstructionHelper.priority(wrapped().getProcessingInstructionsList());
    }
}
