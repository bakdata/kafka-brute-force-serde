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
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.StringConverter;

/**
 * Configuration for kafka-brute-force-connect.
 *
 * <p>
 * It let users configure:
 * <ul>
 *     <li>a list of {@link Converter} that should be tried during the brute-force</li>
 * </ul>
 * </p>
 *
 * See {@link AbstractBruteForceConfig} for more configuration options.
 */
public class BruteForceConverterConfig extends AbstractBruteForceConfig {

    public static final String CONVERTER_CONFIG = PREFIX + "converters";
    public static final String CONVERTER_DOCS = "A comma separated list of converters that should be tried.";

    public static final List<String> CONVERTER_DEFAULTS = List.of(
            "io.confluent.connect.avro.AvroConverter",
            StringConverter.class.getName(),
            ByteArrayConverter.class.getName()
    );

    public static final ConfigDef CONFIG = configDef();

    protected BruteForceConverterConfig(final Map<?, ?> originals) {
        super(CONFIG, originals);
    }

    private static ConfigDef configDef() {
        return baseConfigDef()
                .define(CONVERTER_CONFIG, Type.LIST, CONVERTER_DEFAULTS, Importance.MEDIUM, CONVERTER_DOCS);
    }

    public List<Converter> getConverters() {
        return this.getInstances(CONVERTER_CONFIG, Converter.class);
    }
}
