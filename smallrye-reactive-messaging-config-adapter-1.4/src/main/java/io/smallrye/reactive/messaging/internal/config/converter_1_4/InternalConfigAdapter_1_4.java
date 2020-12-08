package io.smallrye.reactive.messaging.internal.config.converter_1_4;

import java.util.Optional;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.spi.ConfigSource;

import io.smallrye.reactive.messaging.internal.InternalConfig;

/**
 * @author <a href="mailto:kabir.khan@jboss.com">Kabir Khan</a>
 */
class InternalConfigAdapter_1_4 implements Config {
    private final InternalConfig config;

    InternalConfigAdapter_1_4(InternalConfig config) {
        this.config = config;
    }

    @Override
    public <T> T getValue(String propertyName, Class<T> propertyType) {
        return config.getValue(propertyName, propertyType);
    }

    @Override
    public <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType) {
        return config.getOptionalValue(propertyName, propertyType);
    }

    @Override
    public Iterable<String> getPropertyNames() {
        return config.getPropertyNames();
    }

    @Override
    public Iterable<ConfigSource> getConfigSources() {
        return config.getConfigSources();
    }
}
