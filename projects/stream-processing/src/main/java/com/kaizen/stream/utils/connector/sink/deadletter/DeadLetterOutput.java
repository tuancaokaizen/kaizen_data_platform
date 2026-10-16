package com.kaizen.stream.utils.connector.sink.deadletter;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

/**
 * Side output shared by every parser that can reject records.
 * Side output dùng chung cho mọi parser có thể loại record.
 */
public final class DeadLetterOutput {

    public static final OutputTag<DeadLetter> TAG =
            new OutputTag<>("dead-letters", TypeInformation.of(DeadLetter.class));

    private DeadLetterOutput() {
    }
}
