package org.apache.eventmesh.dashboard.console.utils.data.adaptation;

/**
 * @author hahaha
 */
public interface Adaptation<T, V> {

    void handler(T t, V v);
}
