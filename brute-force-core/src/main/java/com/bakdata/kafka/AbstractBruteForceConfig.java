package com.bakdata.kafka;

import io.confluent.common.config.AbstractConfig;
import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigDef.Importance;
import io.confluent.common.config.ConfigDef.Type;
import java.util.Map;

public class AbstractBruteForceConfig extends AbstractConfig {
    public static final String PREFIX = "brute.force.";

    public static final String LARGE_MESSAGE_ENABLED_CONFIG = PREFIX + "large.message.enabled";
    public static final String LARGE_MESSAGE_ENABLED_DOC = "Flag ";
    public static final boolean LARGE_MESSAGE_ENABLED_DEFAULT = true;

    protected AbstractBruteForceConfig(final ConfigDef definition, final Map<?, ?> originals) {
        super(definition, originals);
    }

    protected static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(LARGE_MESSAGE_ENABLED_CONFIG, Type.BOOLEAN, LARGE_MESSAGE_ENABLED_DEFAULT, Importance.LOW,
                        LARGE_MESSAGE_ENABLED_DOC);
    }

    public boolean isLargeMessageEnabled() {
        return this.getBoolean(LARGE_MESSAGE_ENABLED_CONFIG);
    }
}
