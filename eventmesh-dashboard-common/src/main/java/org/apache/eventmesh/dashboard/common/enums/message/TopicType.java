package org.apache.eventmesh.dashboard.common.enums;

/**
 * 
 */
public enum TopicType {

    NORMAL,

    /**
     * 全局顺序消息
     */
    FIFO,

    /**
     * 分区有序
     */
    ORDERLY,

    /**
     * 延迟消息
     */
    DELAY,

    /**
     * 定时消息
     */
    TIMER,

    /**
     * 事务消息
     */
    TRANSACTION,

    /**
     * 轻量消息
     */
    LITE,
}
