package com.gb.didgen.service;

import com.gb.didgen.exception.ClockMovedBackException;
import com.gb.didgen.exception.NodeIdOutOfBoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.Instant;

import static com.gb.didgen.common.Constants.NODE_ID_BIT_LEN;
import static com.gb.didgen.common.Constants.SEQUENCE_BIT_LEN;

@Service
public class SnowflakeSequenceIdGenerator implements SequenceIdGenerator {
    @Autowired
    private Integer generatingNodeId;
    // 64 bits
    //41 bits for time
    // 10 bits for node id
    // 12 bits for sequence
    // 1 signed bit

    // Structure : Timestamp + node id + squence

    // creating NODE_ID_BIT_LEN nodes  and each node can handle  maxSequence (consider
    // as a mix of node is and sequence id gives a id  with some other things )

    private final int maxSequence = (int) Math.pow(2, SEQUENCE_BIT_LEN);// max possible
    // value  a node  can handle (4096 req at the same millisecond )
    private final int maxNodeVal = (int) Math.pow(2, NODE_ID_BIT_LEN);
    private final long EPOCH_START = Instant.EPOCH.toEpochMilli(); // when a req is
    // received


    private volatile long currentSequence = -1L;
    private final Object lock = new Object();
    private volatile long lastTimestamp = -1L;

    @PostConstruct
    public void checkNodeIdBounds() throws NodeIdOutOfBoundException {
        if (generatingNodeId < 0 || generatingNodeId > maxNodeVal) {
            throw new NodeIdOutOfBoundException("Node id is < 0 or > " + maxNodeVal);
        }
    }

    @Override
    public long generateId() throws ClockMovedBackException, NodeIdOutOfBoundException {
        checkNodeIdBounds();
        synchronized (lock) {
            long currentTimeStamp = getTimeStamp();
            if (currentTimeStamp < lastTimestamp) {
                throw new ClockMovedBackException("Clock moved back");
            }
            if (currentTimeStamp == lastTimestamp) {
                currentSequence = currentSequence + 1 & maxSequence;
                if (currentSequence != 0) {
                    currentTimeStamp = waitNextMillis(currentTimeStamp);
                }
            } else {
                currentSequence = 0;
            }
            lastTimestamp = currentTimeStamp;
            long id = currentTimeStamp << (NODE_ID_BIT_LEN + SEQUENCE_BIT_LEN);
            id |= (generatingNodeId << SEQUENCE_BIT_LEN);
            id |= currentSequence;
            return id;
        }
    }

    private long getTimeStamp() {
        return Instant.now().toEpochMilli() - EPOCH_START;
    }

    private long waitNextMillis(long currentTimeStamp) {
        while (currentTimeStamp == lastTimestamp) {
            currentTimeStamp = getTimeStamp();
        }
        return currentTimeStamp;
    }
}
