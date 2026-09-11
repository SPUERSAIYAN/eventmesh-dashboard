package org.apache.eventmesh.dashboard.console.utils.data.adaptation.rocketmq;

import org.apache.eventmesh.dashboard.console.entity.message.TopicEntity;
import org.apache.eventmesh.dashboard.console.model.dto.topic.CreateTopicDTO;
import org.apache.eventmesh.dashboard.console.utils.data.adaptation.operation.CreateAdaptation;
import org.apache.eventmesh.dashboard.console.utils.data.adaptation.type.TopicAdaptation;

/**
 * @author hahaha
 */
public class RocketMQCreateTopicAdaptation implements TopicAdaptation<TopicEntity, CreateTopicDTO>, RocketMQAdaptation<TopicEntity, CreateTopicDTO>,
    CreateAdaptation<TopicEntity, CreateTopicDTO> {

    @Override
    public void adaptation(TopicEntity topicEntity, CreateTopicDTO createTopicDTO) {

    }
}
