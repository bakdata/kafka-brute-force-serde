package com.bakdata.kafka;


import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigDef.Importance;
import io.confluent.common.config.ConfigDef.Type;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;

public class BruteForceSerdeConfig extends AbstractBruteForceConfig {

    public static final String DESERIALIZERS_CONFIG = PREFIX + "deserializers";
    public static final String DESERIALIZERS_DOC = "A comma separated list SerDes that should be tried.";

    public static final List<String> DESERIALIZERS_DEFAULT = List.of(
            SpecificAvroSerde.class.getName(),
            GenericAvroSerde.class.getName(),
            StringSerde.class.getName(),
            ByteArraySerde.class.getName()
    );

    public static final ConfigDef CONFIG = configDef();

    private static ConfigDef configDef() {
        return baseConfigDef()
                .define(DESERIALIZERS_CONFIG, Type.LIST, DESERIALIZERS_DEFAULT, Importance.MEDIUM, DESERIALIZERS_DOC);
    }

    public BruteForceSerdeConfig(final Map<?, ?> originals) {
        super(CONFIG, originals);
    }

    public List<Serde<?>> getSerdes() {
        return this.getConfiguredInstances(DESERIALIZERS_CONFIG, Serde.class).stream()
                .map(serde -> (Serde<?>) serde)
                .collect(Collectors.toList());
    }

}
