package io.smallrye.reactive.messaging.internal.config.converter_2_0;

import java.util.Optional;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigValue;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.eclipse.microprofile.config.spi.Converter;

import io.smallrye.reactive.messaging.internal.InternalConfig;

/**
 * @author <a href="mailto:kabir.khan@jboss.com">Kabir Khan</a>
 */
class InternalConfigAdapter_2_0 implements Config {
    private final InternalConfig config;

    InternalConfigAdapter_2_0(InternalConfig config) {
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

    @Override
    public ConfigValue getConfigValue(String propertyName) {
        return config.getConfigValue(propertyName);
    }

    @Override
    public <T> Optional<Converter<T>> getConverter(Class<T> aClass) {
        return config.getConverter(aClass);
    }

    @Override
    public <T> T unwrap(Class<T> aClass) {
        return config.unwrap(aClass);
    }
}
