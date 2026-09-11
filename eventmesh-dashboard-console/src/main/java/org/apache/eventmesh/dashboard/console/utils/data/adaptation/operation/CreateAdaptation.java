package org.apache.eventmesh.dashboard.console.utils.data.adaptation;


/**
 * @author hahaha
 */
public interface CreateAdaptation<T, V> extends Adaptation<T, V> {

    default String operation() {
        return "create";
    }
}
