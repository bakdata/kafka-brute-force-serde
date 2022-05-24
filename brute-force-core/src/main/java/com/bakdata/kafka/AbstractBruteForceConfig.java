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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.Utils;

/**
 * Base configuration for kafka-brute-force projects.
 *
 * <p>
 * This allows users to:
 *  <ul>
 *      <li>toggle support for large message</li>
 *      <li>toggle if no matches should lead to an exception</li>
 *  </ul>
 * </p>
 */
public class AbstractBruteForceConfig extends AbstractConfig {
    public static final String PREFIX = "brute.force.";

    public static final String LARGE_MESSAGE_ENABLED_CONFIG = PREFIX + "large.message.enabled";
    public static final String LARGE_MESSAGE_ENABLED_DOC = "Flag for enabling support for large-message-serde.";
    public static final boolean LARGE_MESSAGE_ENABLED_DEFAULT = true;

    public static final String IGNORE_NO_MATCH_CONFIG = PREFIX + "ignore.no.match";
    public static final String IGNORE_NO_MATCH_DOC =
            "If set, the deserialization won't fail and instead keep the data as a byte array. "
                    + "This is equivalent to including ByteArray conversion in the corresponding conversion list.";
    public static final boolean IGNORE_NO_MATCH_DEFAULT = true;

    protected AbstractBruteForceConfig(final ConfigDef definition, final Map<?, ?> originals) {
        super(definition, originals);
    }

    protected static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(LARGE_MESSAGE_ENABLED_CONFIG, Type.BOOLEAN, LARGE_MESSAGE_ENABLED_DEFAULT, Importance.LOW,
                        LARGE_MESSAGE_ENABLED_DOC)
                .define(IGNORE_NO_MATCH_CONFIG, Type.BOOLEAN, IGNORE_NO_MATCH_DEFAULT, Importance.LOW,
                        IGNORE_NO_MATCH_DOC);
    }

    public boolean largeMessageEnabled() {
        return this.getBoolean(LARGE_MESSAGE_ENABLED_CONFIG);
    }

    public boolean ignoreNoMatch() {
        return this.getBoolean(IGNORE_NO_MATCH_CONFIG);
    }

    /**
     * Get a list of instances as configured by the given config.
     *
     * <p>
     * Compared to {@link #getConfiguredInstances(String, Class)}, this method doesn't call
     * {@link org.apache.kafka.common.Configurable#configure(Map)}. This allows to defer the configuration.
     * </p>
     *
     * @param config key for the list of classes
     * @param baseClazz base class for all instances
     * @return a list with instances of the base class
     */
    protected <T> List<T> getInstances(final String config, final Class<T> baseClazz) {
        return this.getList(config).stream().map(configuredClazz -> {
                    try {
                        return (Class<? extends T>) Utils.loadClass(configuredClazz, baseClazz);
                    } catch (final ClassNotFoundException e) {
                        throw new KafkaException("Could not load class " + configuredClazz);
                    }
                }).map(Utils::newInstance)
                .collect(Collectors.toList());
    }

}
