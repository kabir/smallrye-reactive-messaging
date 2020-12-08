package io.smallrye.reactive.messaging.impl;

import static io.smallrye.reactive.messaging.i18n.ProviderExceptions.ex;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import io.smallrye.reactive.messaging.internal.InternalConfigAdapterFactory;

/**
 * @author <a href="mailto:kabir.khan@jboss.com">Kabir Khan</a>
 */
class InternalConfigAdapterFactoryUtil {
    static InternalConfigAdapterFactory findInternalConfigAdapterFactory() {
        final ClassLoader classLoader;
        if (System.getSecurityManager() == null) {
            classLoader = Thread.currentThread().getContextClassLoader();
        } else {
            classLoader = AccessController.doPrivileged(
                    (PrivilegedAction<ClassLoader>) () -> Thread.currentThread().getContextClassLoader());
        }

        List<InternalConfigAdapterFactory> list = new ArrayList<>();
        ServiceLoader.load(InternalConfigAdapterFactory.class, classLoader).forEach(v -> list.add(v));

        if (list.size() == 1) {
            return list.get(0);
        }

        throw ex.serviceLoaderExpectesOneEntry(InternalConfigAdapterFactory.class, list);
    }
}
