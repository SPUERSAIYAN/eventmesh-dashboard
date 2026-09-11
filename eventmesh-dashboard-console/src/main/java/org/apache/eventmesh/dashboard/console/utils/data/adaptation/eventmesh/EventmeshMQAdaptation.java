package org.apache.eventmesh.dashboard.console.utils.data.adaptation.eventmesh.topic;

import org.apache.eventmesh.dashboard.common.enums.ClusterType;
import org.apache.eventmesh.dashboard.console.utils.data.adaptation.Adaptation;

/**
 * @author hahaha
 */
public interface EventmeshMQAdaptation<T, V> extends Adaptation<T, V> {

    @Override
    default ClusterType[] clusterType() {
        return new ClusterType[] {ClusterType.STORAGE_ROCKETMQ_CLUSTER};
    }

}
