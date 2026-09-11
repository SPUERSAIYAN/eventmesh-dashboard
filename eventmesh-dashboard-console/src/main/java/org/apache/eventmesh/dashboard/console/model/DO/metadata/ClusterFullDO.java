package org.apache.eventmesh.dashboard.console.model.DO.metadata;

import org.apache.eventmesh.dashboard.console.entity.cluster.ClusterEntity;

import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ClusterFullDO extends BaseFullDO {

    private ClusterEntity clusterEntity;

    private List<RuntimeFullDO> runtimeFullDOList;

}
