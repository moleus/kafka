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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AckedOffsetsTest {

    private final TopicPartition tp = new TopicPartition("topic", 0);

    @Test
    public void testUnknownBeforeThePositionIsKnown() {
        AckedOffsets acked = new AckedOffsets();
        assertEquals(-1L, acked.nextOffset(tp));
        assertFalse(acked.positionKnown(tp));
        acked.seek(tp, 10);
        assertEquals(10L, acked.nextOffset(tp));
        assertTrue(acked.positionKnown(tp));
    }

    @Test
    public void testHoldsAtTheLowestUnacknowledgedOffset() {
        AckedOffsets acked = new AckedOffsets();
        acked.seek(tp, 10);
        acked.polled(tp, List.of(10L, 11L, 12L), 13);
        assertEquals(10L, acked.nextOffset(tp));
        acked.acked(tp, 11);
        assertEquals(10L, acked.nextOffset(tp));
        acked.acked(tp, 10);
        assertEquals(12L, acked.nextOffset(tp));
        acked.acked(tp, 12);
        assertEquals(13L, acked.nextOffset(tp));
    }

    @Test
    public void testMovesPastOffsetsWithoutRecords() {
        AckedOffsets acked = new AckedOffsets();
        // Offsets 21 to 24 were compacted away and offset 26 is a transaction marker.
        acked.polled(tp, List.of(20L, 25L), 27);
        acked.acked(tp, 20);
        assertEquals(25L, acked.nextOffset(tp));
        acked.acked(tp, 25);
        assertEquals(27L, acked.nextOffset(tp));
        // A poll that only moved past a marker returns no records.
        acked.polled(tp, List.of(), 29);
        assertEquals(29L, acked.nextOffset(tp));
    }

    @Test
    public void testIgnoresUnknownAndRepeatedAcknowledgements() {
        AckedOffsets acked = new AckedOffsets();
        acked.polled(tp, List.of(5L, 6L), 7);
        acked.acked(tp, 3);
        acked.acked(tp, 9);
        assertEquals(5L, acked.nextOffset(tp));
        acked.acked(tp, 5);
        acked.acked(tp, 5);
        assertEquals(6L, acked.nextOffset(tp));
    }

    @Test
    public void testNeverRunsAheadOfAcknowledgements() {
        Random random = new Random(42);
        for (int run = 0; run < 500; run++) {
            AckedOffsets acked = new AckedOffsets();
            long position = random.nextInt(5);
            acked.seek(tp, position);
            List<Long> unacked = new ArrayList<>();
            Set<Long> records = new HashSet<>();
            for (int step = 0; step < 60; step++) {
                if (random.nextBoolean()) {
                    List<Long> offsets = new ArrayList<>();
                    for (int i = random.nextInt(4); i > 0; i--) {
                        // A gap of one or two offsets stands for compacted offsets and markers.
                        position += random.nextInt(3);
                        offsets.add(position++);
                    }
                    position += random.nextInt(2);
                    acked.polled(tp, offsets, position);
                    unacked.addAll(offsets);
                    records.addAll(offsets);
                } else if (!unacked.isEmpty()) {
                    acked.acked(tp, unacked.remove(random.nextInt(unacked.size())));
                }
                long next = acked.nextOffset(tp);
                assertTrue(next <= position);
                for (long offset : unacked) {
                    assertTrue(offset >= next, "offset " + offset + " is not acknowledged, but the next offset is " + next);
                }
                if (unacked.isEmpty()) {
                    assertEquals(position, next);
                }
            }
        }
    }
}
