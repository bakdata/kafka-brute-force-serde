[![Build Status](https://dev.azure.com/bakdata/public/_apis/build/status/bakdata.kafka-brute-force-serde?repoName=bakdata%2Fkafka-brute-force-serde&branchName=main)](https://dev.azure.com/bakdata/public/_build/latest?definitionId=30&repoName=bakdata%2Fkafka-brute-force-serde&branchName=main)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.kafka%3Abrute-force&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=com.bakdata.kafka%3Abrute-force)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=com.bakdata.kafka%3Abrute-force&metric=coverage)](https://sonarcloud.io/summary/new_code?id=com.bakdata.kafka%3Abrute-force)
[![Maven](https://img.shields.io/maven-central/v/com.bakdata.kafka/brute-force-serde.svg)](https://search.maven.org/search?q=g:com.bakdata.kafka%20AND%20a:brute-force-serde&core=gav)

# kafka-brute-force-serde
A Kafka SerDe to deserialize messages of an unknown serialization format

## Getting Started

### Serde

You can add kafka-brute-force-serde via Maven Central.

#### Gradle
```gradle
implementation group: 'com.bakdata.kafka', name: 'brute-force-serde', version: '1.0.0'
```

#### Maven
```xml
<dependency>
    <groupId>com.bakdata.kafka</groupId>
    <artifactId>brute-force-serde</artifactId>
    <version>1.0.0</version>
</dependency>
```

For other build tools or versions, refer to the [latest version in MvnRepository](https://mvnrepository.com/artifact/com.bakdata.kafka/brute-force-serde/latest).

Make sure to also add [Confluent Maven Repository](http://packages.confluent.io/maven/) to your build file.

#### Usage

You can use it from your Kafka Streams application like any other Serde

```java
final Serde<Object> serde = new BruteForceSerde<>();
serde.configure(Map.of(), false);
```

The Serde attempts to deserialize messages using the following Serdes in this order:
- SpecificAvroSerde (if `schema.registry.url`) is present in the Serde configuration
- GenericAvroSerde (if `schema.registry.url`) is present in the Serde configuration
- StringSerde
- ByteArraySerde

For each serialization format, BruteForceSerde first attempts deserialization using [Kafka Large Message Serde](https://github.com/bakdata/kafka-large-message-serde/)
and then uses the standard format.

All configuration properties are delegated to the nested Serdes used by BruteForceSerde.

### Kafka Connect

This serde also comes with support for Kafka Connect.
You can add kafka-brute-force-connect via Maven Central.

#### Gradle
```gradle
implementation group: 'com.bakdata.kafka', name: 'brute-force-connect', version: '1.0.0'
```

#### Maven
```xml
<dependency>
    <groupId>com.bakdata.kafka</groupId>
    <artifactId>brute-force-connect</artifactId>
    <version>1.0.0</version>
</dependency>
```

For other build tools or versions, refer to the [latest version in MvnRepository](https://mvnrepository.com/artifact/com.bakdata.kafka/brute-force-connect/latest).

#### Usage

To use it with your Kafka Connect connectors, just configure your converter as `com.bakdata.kafka.BruteForceConverter`.

The Serde attempts to deserialize messages using the following Serdes in this order:
- AvroConverter (if `schema.registry.url`) is present in the Serde configuration
- StringConverter
- ByteArrayConverter

For each serialization format, BruteForceSerde first attempts deserialization using [Kafka Large Message Converter](https://github.com/bakdata/kafka-large-message-serde/)
and then uses the standard format.

All configuration properties are delegated to the nested Converters used by BruteForceConverter.

For general guidance on how to configure Kafka Connect converters, please have a look at the [official documentation](https://docs.confluent.io/home/connect/configuring.html).

## Development

If you want to contribute to this project, you can simply clone the repository and build it via Gradle.
All dependencies should be included in the Gradle files, there are no external prerequisites.

```bash
> git clone git@github.com:bakdata/kafka-brute-force-serde.git
> cd kafka-brute-force-serde && ./gradlew build
```

Please note, that we have [code styles](https://github.com/bakdata/bakdata-code-styles) for Java.
They are basically the Google style guide, with some small modifications.

## Contributing

We are happy if you want to contribute to this project.
If you find any bugs or have suggestions for improvements, please open an issue.
We are also happy to accept your PRs.
Just open an issue beforehand and let us know what you want to do and why.

## License
This project is licensed under the MIT license.
Have a look at the [LICENSE](https://github.com/bakdata/kafka-brute-force-serde/blob/main/LICENSE) for more details.

