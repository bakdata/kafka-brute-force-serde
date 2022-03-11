package com.bakdata.kafka;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.StringConverter;

@NoArgsConstructor
@Slf4j
public class BruteForceConverter implements Converter {
    private static final List<Supplier<Converter>> DEFAULT_FACTORIES = List.of(
            StringConverter::new,
            ByteArrayConverter::new
    );

    private List<Converter> converters;

    private static Stream<Converter> createConverters(final Map<String, ?> configs, final boolean isKey,
            final Supplier<? extends Converter> factory) {
        final Converter converter = factory.get();
        converter.configure(configs, isKey);
        final Converter largeMessageConverter = new LargeMessageConverter();
        final Map<String, Object> largeMessageConfigs = createLargeMessageConfig(configs, converter);
        largeMessageConverter.configure(largeMessageConfigs, isKey);
        return Stream.of(largeMessageConverter, converter);
    }

    private static Map<String, Object> createLargeMessageConfig(final Map<String, ?> configs,
            final Converter converter) {
        final Map<String, Object> conf = new HashMap<>(configs);
        conf.put(LargeMessageConverterConfig.CONVERTER_CLASS_CONFIG, converter.getClass());
        return conf;
    }

    private static Stream<Supplier<Converter>> getFactories(final Map<String, ?> configs) {
        final Collection<Supplier<Converter>> factories = new ArrayList<>();
        if (configs.containsKey(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG)) {
            factories.add(AvroConverter::new);
        }
        factories.addAll(DEFAULT_FACTORIES);
        return factories.stream();
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        this.converters = getFactories(configs)
                .flatMap(factory -> createConverters(configs, isKey, factory))
                .collect(Collectors.toList());
    }

    @Override
    public byte[] fromConnectData(final String topic, final Schema schema, final Object value) {
        throw new SerializationException("BruteForceConverter only supports converting to connect data");
    }

    @Override
    public SchemaAndValue toConnectData(final String topic, final byte[] value) {
        Objects.requireNonNull(this.converters);
        for (final Converter converter : this.converters) {
            final Class<? extends Converter> clazz = converter.getClass();
            try {
                final SchemaAndValue s = converter.toConnectData(topic, value);
                log.trace("Converted message using {}", clazz);
                return s;
            } catch (final RuntimeException ex) {
                log.trace(String.format("Failed converting message using %s", clazz), ex);
            }
        }
        throw new IllegalStateException("Conversion should have worked with " + ByteArrayConverter.class);
    }
}
