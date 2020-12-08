package io.smallrye.reactive.messaging.internal;

import org.eclipse.microprofile.config.Config;

/**
 * @author <a href="mailto:kabir.khan@jboss.com">Kabir Khan</a>
 */
public interface InternalConfigAdapterFactory {
    Config create(InternalConfig internalConfig);
}
