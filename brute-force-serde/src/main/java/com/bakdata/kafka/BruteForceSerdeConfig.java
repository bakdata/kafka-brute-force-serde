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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;

/**
 * Configuration for kafka-brute-force-serde.
 *
 * <p>
 * It let users configure:
 * <ul>
 *     <li>a list of {@link Serde} that should tried during the brute-force</li>
 * </ul>
 *
 * See {@link AbstractBruteForceConfig} for more configuration options.
 */
public class BruteForceSerdeConfig extends AbstractBruteForceConfig {

    public static final String SERDES_CONFIG = PREFIX + "serdes";
    public static final String SERDES_DOC = "A comma separated list of SerDes that should be tried.";

    public static final List<String> SERDES_DEFAULT = List.of(
            "io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde",
            "io.confluent.kafka.streams.serdes.avro.GenericAvroSerde",
            StringSerde.class.getName(),
            ByteArraySerde.class.getName()
    );

    public static final ConfigDef CONFIG = configDef();

    /**
     * Create config from the given properties.
     *
     * @param originals the properties
     */
    public BruteForceSerdeConfig(final Map<?, ?> originals) {
        super(CONFIG, originals);
    }


    private static ConfigDef configDef() {
        return baseConfigDef()
                .define(SERDES_CONFIG, Type.LIST, SERDES_DEFAULT, Importance.MEDIUM, SERDES_DOC);
    }

    /**
     * Instantiates and returns the list of configured serde classes.
     */
    @SuppressWarnings("unchecked")
    public List<Serde<Object>> getSerdes() {
        return this.getInstances(SERDES_CONFIG, Serde.class).stream()
                .map(serde -> (Serde<Object>) serde)
                .collect(Collectors.toList());
    }

}
