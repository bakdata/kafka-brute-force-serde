package com.bakdata.kafka;

import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigDef.Importance;
import io.confluent.common.config.ConfigDef.Type;
import io.confluent.connect.avro.AvroConverter;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.StringConverter;

public class BruteForceConverterConfig extends AbstractBruteForceConfig {

    public static final String CONVERTER_CONFIG = PREFIX + "deserializers";
    public static final String CONVERTER_DOCS = "A comma separated list of converters that should be tried.";

    public static final List<String> CONVERTER_DEFAULTS = List.of(
            AvroConverter.class.getName(),
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
        return this.getConfiguredInstances(CONVERTER_CONFIG, Converter.class);
    }
}
