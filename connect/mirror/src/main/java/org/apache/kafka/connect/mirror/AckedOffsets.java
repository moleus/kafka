/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.mirror;

import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The next source offset per partition that the target did not acknowledge yet.
 * <p>
 * Every offset below this value is in one of three groups: the target acknowledged its record
 * in this task, an earlier task committed it, or the consumer returns no record for it (a control
 * record, an aborted or a compacted offset). So the value never runs ahead of delivery: a polled
 * record holds it back until its acknowledgement, in any order of acknowledgements.
 * <p>
 * The task thread calls {@link #seek} and {@link #polled}, the producer I/O thread calls
 * {@link #acked}, and a metrics reporter calls {@link #nextOffset}.
 */
class AckedOffsets {

    private final Map<TopicPartition, PartitionState> partitions = new ConcurrentHashMap<>();

    /** Sets the position at the task start: the committed source offset plus one. */
    void seek(TopicPartition topicPartition, long position) {
        state(topicPartition).polled(List.of(), position);
    }

    /**
     * Holds back the offsets of the records that one poll returned until the target acknowledges
     * them, and moves the position to the consumer position after that poll.
     */
    void polled(TopicPartition topicPartition, List<Long> offsets, long position) {
        state(topicPartition).polled(offsets, position);
    }

    void acked(TopicPartition topicPartition, long offset) {
        state(topicPartition).acked(offset);
    }

    /** The next unacknowledged source offset, or -1 before the position is known. */
    long nextOffset(TopicPartition topicPartition) {
        return state(topicPartition).nextOffset();
    }

    boolean positionKnown(TopicPartition topicPartition) {
        return state(topicPartition).position >= 0;
    }

    private PartitionState state(TopicPartition topicPartition) {
        return partitions.computeIfAbsent(topicPartition, tp -> new PartitionState());
    }

    private static final class PartitionState {
        private volatile long position = -1L;
        // Polled offsets that wait for an acknowledgement.
        private final TreeSet<Long> pending = new TreeSet<>();

        synchronized void polled(List<Long> offsets, long nextPosition) {
            pending.addAll(offsets);
            position = nextPosition;
        }

        synchronized void acked(long offset) {
            pending.remove(offset);
        }

        synchronized long nextOffset() {
            return pending.isEmpty() ? position : Math.min(pending.first(), position);
        }
    }
}
