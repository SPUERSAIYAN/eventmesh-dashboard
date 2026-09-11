package org.apache.eventmesh.dashboard.console.utils.data.adaptation.eventmesh.topic;

import org.apache.eventmesh.dashboard.console.entity.message.TopicEntity;
import org.apache.eventmesh.dashboard.console.model.dto.topic.CreateTopicDTO;
import org.apache.eventmesh.dashboard.console.utils.data.adaptation.operation.UpdateAdaptation;
import org.apache.eventmesh.dashboard.console.utils.data.adaptation.rocketmq.RocketMQAdaptation;
import org.apache.eventmesh.dashboard.console.utils.data.adaptation.type.TopicAdaptation;

/**
 * @author hahaha
 */
public class RocketMQCreateUpdateAdaptation implements TopicAdaptation<TopicEntity, CreateTopicDTO>, RocketMQAdaptation<TopicEntity, CreateTopicDTO>,
    UpdateAdaptation<TopicEntity, CreateTopicDTO> {

    @Override
    public void adaptation(TopicEntity topicEntity, CreateTopicDTO createTopicDTO) {

    }
}
