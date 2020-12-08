package io.smallrye.reactive.messaging.internal;

import java.util.Optional;

import org.eclipse.microprofile.config.ConfigValue;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.eclipse.microprofile.config.spi.Converter;

/**
 * Internal interface to support MP Config 1.4, 2.0 etc.
 *
 * Contains all the methods from the 1.4 Config interface, and as much as possible from the 2.0 interface
 *
 * @author <a href="mailto:kabir.khan@jboss.com">Kabir Khan</a>
 */
public interface InternalConfig {
    // 1.4 methods
    <T> T getValue(String propertyName, Class<T> propertyType);

    <T> Optional<T> getOptionalValue(String propertyName, Class<T> propertyType);

    Iterable<String> getPropertyNames();

    Iterable<ConfigSource> getConfigSources();

    // 1.2 methods
    ConfigValue getConfigValue(String s);

    <T> T unwrap(Class<T> aClass);

    <T> Optional<Converter<T>> getConverter(Class<T> aClass);
}
