package io.axoniq.axonserver.cluster.replication.file;

import io.axoniq.axonserver.grpc.cluster.Config;
import io.axoniq.axonserver.grpc.cluster.Entry;
import io.axoniq.axonserver.grpc.cluster.SerializedObject;
import io.axoniq.axonserver.grpc.internal.TransactionWithToken;
import org.junit.*;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Author: marc
 */
public class LogEntryFileTest {

    @Test
    @Ignore("Manual test only")
    public void readLogFile() throws IOException {
        try (DataInputStream dataInputStream = new DataInputStream(new FileInputStream(
                                                              "D:\\test\\loadtest\\axonhub\\log\\default\\00000000000001.log"))) {

            dataInputStream.read();
            dataInputStream.readInt();
            int size = dataInputStream.readInt();
            long index = 249027L;
            while (size > 0) {
                dataInputStream.read(); // version
                long term = dataInputStream.readLong();
                int type = dataInputStream.readInt();
                Entry.DataCase dataCase = Entry.DataCase.forNumber(type);
                byte[] bytes = new byte[size];
                dataInputStream.read(bytes);
                Entry.Builder builder = Entry.newBuilder().setTerm(term).setIndex(index);
                switch (dataCase) {
                    case SERIALIZEDOBJECT:
                        SerializedObject so = SerializedObject.parseFrom(bytes);
                        TransactionWithToken twt = TransactionWithToken.parseFrom(so.getData());
                        System.out.println( twt.getToken() + " " + twt.getEventsCount());
                        builder.setSerializedObject(so);
                        break;
                    case NEWCONFIGURATION:
                        builder.setNewConfiguration(Config.parseFrom(bytes));
                        break;
                    case DATA_NOT_SET:
                        break;
                }
                dataInputStream.readInt(); //CRC
                size = dataInputStream.readInt();
                System.out.print(builder.build());
                index++;
            }

        }
    }


    @Test
    @Ignore("Manual test only")
    public void readEventsFile() throws IOException {
        try (DataInputStream dataInputStream = new DataInputStream(new FileInputStream(
                "D:\\test\\loadtest\\axonhub2\\data\\default\\00000000000000.events"))) {

            dataInputStream.read();
            dataInputStream.readInt();
            int size = dataInputStream.readInt();
            int index = 0;
            while (size > 0) {
                dataInputStream.read(); // version
                long idx = dataInputStream.readLong();
                short eventCount = dataInputStream.readShort();
                for( int i = 0 ; i < eventCount; i++) {
                    int eventSize = dataInputStream.readInt();
                    byte[] bytes = new byte[eventSize];
                    dataInputStream.read(bytes);
                }
                dataInputStream.readInt(); //CRC
                size = dataInputStream.readInt();
                System.out.println("Transaction " + idx + ", nrEvents: " + eventCount);
                index+= eventCount;
            }

        }
    }

}
