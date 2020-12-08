package io.smallrye.reactive.messaging.internal.config.converter_1_4;

import org.eclipse.microprofile.config.Config;

import io.smallrye.reactive.messaging.internal.InternalConfig;
import io.smallrye.reactive.messaging.internal.InternalConfigAdapterFactory;

/**
 * @author <a href="mailto:kabir.khan@jboss.com">Kabir Khan</a>
 */
public class InternalConfigAdapterFactory_1_4 implements InternalConfigAdapterFactory {
    @Override
    public Config create(InternalConfig internalConfig) {
        return new InternalConfigAdapter_1_4(internalConfig);
    }
}
