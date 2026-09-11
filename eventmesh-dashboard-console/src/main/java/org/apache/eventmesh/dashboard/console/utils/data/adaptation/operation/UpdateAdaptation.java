package org.apache.eventmesh.dashboard.console.utils.data.adaptation.operation;


import org.apache.eventmesh.dashboard.console.utils.data.adaptation.Adaptation;

/**
 * @author hahaha
 */
public interface UpdateAdaptation<T, V> extends Adaptation<T, V> {

    default String operation() {
        return "create";
    }
}
