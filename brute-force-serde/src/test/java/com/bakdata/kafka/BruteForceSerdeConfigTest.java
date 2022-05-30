/*
 * MIT License
 *
 * Copyright (c) 2022 bakdata GmbH
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.bakdata.kafka;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.junit.jupiter.api.Test;

class BruteForceSerdeConfigTest {

    @Test
    void shouldGetDefaultSerdes() {
        final BruteForceSerdeConfig config = new BruteForceSerdeConfig(Map.of());
        assertThat(config.getSerdes())
                .hasExactlyElementsOfTypes(SpecificAvroSerde.class, GenericAvroSerde.class, StringSerde.class,
                        ByteArraySerde.class);
    }

    @Test
    void shouldGetConfiguredSerdes() {
        final BruteForceSerdeConfig config = new BruteForceSerdeConfig(Map.of(
                "brute.force.serdes",
                List.of(KafkaProtobufSerde.class.getName(), KafkaJsonSchemaSerde.class.getName())
        ));
        assertThat(config.getSerdes())
                .hasExactlyElementsOfTypes(KafkaProtobufSerde.class, KafkaJsonSchemaSerde.class);
    }
}
