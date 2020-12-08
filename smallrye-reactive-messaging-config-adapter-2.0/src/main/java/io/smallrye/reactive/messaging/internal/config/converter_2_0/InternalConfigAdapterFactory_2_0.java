package io.smallrye.reactive.messaging.internal.config.converter_2_0;

import org.eclipse.microprofile.config.Config;

import io.smallrye.reactive.messaging.internal.InternalConfig;
import io.smallrye.reactive.messaging.internal.InternalConfigAdapterFactory;

/**
 * @author <a href="mailto:kabir.khan@jboss.com">Kabir Khan</a>
 */
public class InternalConfigAdapterFactory_2_0 implements InternalConfigAdapterFactory {
    @Override
    public Config create(InternalConfig internalConfig) {
        return new InternalConfigAdapter_2_0(internalConfig);
    }
}
