package io.axoniq.axonserver.grpc;

import com.google.protobuf.AbstractParser;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.axoniq.axonserver.ProcessingInstructionHelper;
import io.axoniq.axonserver.grpc.command.Command;
import org.apache.commons.compress.utils.IOUtils;

import java.io.IOException;
import java.io.InputStream;

/**
 * Author: marc
 */
public class SerializedCommand extends SerializedMessage<Command> {

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

    public SerializedCommand(InputStream inputStream) {
        try {
            serializedData = IOUtils.toByteArray(inputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public SerializedCommand(byte[] toByteArray, String client, String messageId) {
        this(toByteArray);
        this.client = client;
        this.messageId = messageId;
    }

    public static SerializedCommand getDefaultInstance() {
        return new SerializedCommand(Command.getDefaultInstance());
    }

    @Override
    public void writeTo(CodedOutputStream output) throws IOException {
        output.write(serializedData, 0, serializedData.length);
    }

    @Override
    public int getSerializedSize() {
        return serializedData.length;
    }

    @Override
    public byte[] toByteArray() {
        return serializedData;
    }

    @Override
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

    @Override
    public Parser<? extends Message> getParserForType() {
        return new AbstractParser<Message>() {
            @Override
            public Message parsePartialFrom(CodedInputStream codedInputStream,
                                            ExtensionRegistryLite extensionRegistryLite)
                    throws InvalidProtocolBufferException {
                try {
                    return new SerializedCommand(codedInputStream.readByteArray());
                } catch (IOException e) {
                    throw new InvalidProtocolBufferException(e);

                }
            }
        };
    }

    @Override
    public Message.Builder newBuilderForType() {
        return new Builder();
    }

    @Override
    public Message.Builder toBuilder() {
        return new Builder().setCommand(command);
    }

    @Override
    public Message getDefaultInstanceForType() {
        return getDefaultInstance();
    }

    @Override
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

    public static class Builder extends com.google.protobuf.GeneratedMessageV3.Builder<SerializedCommandProviderInbound.Builder> {
        private Command command;

        @Override
        protected GeneratedMessageV3.FieldAccessorTable internalGetFieldAccessorTable() {
            return null;
        }

        @Override
        public Message build() {
            return new SerializedCommand(command);
        }

        @Override
        public Message buildPartial() {
            return new SerializedCommand(command);
        }

        @Override
        public Message getDefaultInstanceForType() {
            return SerializedCommand.getDefaultInstance();
        }

        public Builder setCommand(Command command) {
            this.command = command;
            return this;
        }
    }
}
