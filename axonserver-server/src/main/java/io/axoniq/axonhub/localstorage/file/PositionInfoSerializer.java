package io.axoniq.axonhub.localstorage.file;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Author: marc
 */
public class PositionInfoSerializer implements Serializer<SortedSet<PositionInfo>> {
    private static final PositionInfoSerializer positionInfoSerializer = new PositionInfoSerializer();

    public static PositionInfoSerializer get() {
        return positionInfoSerializer;
    }


    @Override
    public void serialize(DataOutput2 out, SortedSet<PositionInfo> value) throws IOException {
        out.packInt(value.size());
        out.packLong(value.first().getAggregateSequenceNumber());
        for(PositionInfo positionInfo: value) {
            out.packInt(positionInfo.getPosition());
        }
    }

    @Override
    public SortedSet<PositionInfo> deserialize(DataInput2 input, int available) throws IOException {
        int count = input.unpackInt();
        long sequenceNumber = input.unpackLong();
        SortedSet<PositionInfo> values = new ConcurrentSkipListSet<>();
        for( int i =0; i < count; i++) {
           values.add( new PositionInfo(input.unpackInt(), sequenceNumber++));
        }
        return values;
    }
}
