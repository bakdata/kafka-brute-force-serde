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

import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.junit.jupiter.api.Test;

class AbstractBruteForceConfigTest {

    @Test
    void shouldHaveDefaultForLargeMessageEnabled() {
        final TestBruteForceConfig testBruteForceConfig = new TestBruteForceConfig(Map.of());
        assertThat(testBruteForceConfig.isLargeMessageEnabled()).isTrue();
    }

    @Test
    void shouldHaveDefaultForIgnoreNoMatch() {
        final TestBruteForceConfig testBruteForceConfig = new TestBruteForceConfig(Map.of());
        assertThat(testBruteForceConfig.shouldIgnoreNoMatch()).isTrue();
    }

    @Test
    void shouldSetLargeMessageEnabled() {
        final Map<String, Object> config = Map.of("brute.force.large.message.enabled", false);
        final TestBruteForceConfig testBruteForceConfig = new TestBruteForceConfig(config);
        assertThat(testBruteForceConfig.isLargeMessageEnabled()).isFalse();
    }

    @Test
    void shouldSetShouldIgnoreMatch() {
        final Map<String, Object> config = Map.of("brute.force.ignore.no.match", false);
        final TestBruteForceConfig testBruteForceConfig = new TestBruteForceConfig(config);
        assertThat(testBruteForceConfig.shouldIgnoreNoMatch()).isFalse();
    }

    @Test
    void shouldGetInstances() {
        final Map<String, Object> config = Map.of("test.instance", List.of(TestInstanceClass.class.getName()));
        final TestBruteForceConfig testBruteForceConfig = new TestBruteForceConfig(config);
        final List<TestInstanceClass> instances =
                testBruteForceConfig.getInstances("test.instance", TestInstanceClass.class);
        assertThat(instances)
                .hasSize(1)
                .hasExactlyElementsOfTypes(TestInstanceClass.class);
    }

    @Test
    void shouldThrowExceptionIfClassIsUnknown() {
        final Map<String, Object> config = Map.of("test.instance", List.of("some.random.ClassName"));
        final TestBruteForceConfig testBruteForceConfig = new TestBruteForceConfig(config);
        assertThatExceptionOfType(KafkaException.class)
                .isThrownBy(() -> testBruteForceConfig.getInstances("test.instance", TestInstanceClass.class))
                .withMessage("Class some.random.ClassName cannot be found")
                .withCauseInstanceOf(ClassNotFoundException.class);
    }


    public static class TestInstanceClass {}

    static class TestBruteForceConfig extends AbstractBruteForceConfig {
        protected TestBruteForceConfig(final Map<?, ?> originals) {
            super(baseConfigDef().define("test.instance", Type.LIST, null, Importance.HIGH, "Test instances"),
                    originals);
        }
    }

}
