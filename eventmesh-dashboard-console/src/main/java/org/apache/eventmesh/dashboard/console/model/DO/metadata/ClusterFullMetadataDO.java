package org.apache.eventmesh.dashboard.console.model.DO.metadata;

import org.apache.eventmesh.dashboard.console.entity.cluster.ClusterEntity;
import org.apache.eventmesh.dashboard.console.entity.cluster.ClusterRelationshipEntity;
import org.apache.eventmesh.dashboard.console.entity.cluster.RuntimeEntity;
import org.apache.eventmesh.dashboard.console.entity.function.ConfigEntity;
import org.apache.eventmesh.dashboard.console.entity.message.GroupEntity;
import org.apache.eventmesh.dashboard.console.entity.message.TopicEntity;

import java.util.List;

import lombok.Data;

/**
 * @author hahaha
 */
@Data
public class ClusterFullMatedataDO {

    private List<ClusterEntity> clusterEntityList;

    private List<ClusterRelationshipEntity> clusterRelationshipEntityList;

    private List<RuntimeEntity> runtimeEntityList;

    private List<TopicEntity> topicEntityList;

    private List<GroupEntity> groupEntityList;

    private List<ConfigEntity> configEntityList;

}
