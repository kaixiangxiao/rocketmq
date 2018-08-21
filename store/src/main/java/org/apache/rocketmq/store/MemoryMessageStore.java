package org.apache.rocketmq.store;

import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemoryMessageStore implements MessageStore {

    private boolean start = false;

    private boolean shutdown = false;

    private ConcurrentHashMap<String/*Topic*/, List<LinkedBlockingQueue<Message>/*Mem Queue*/>>/*Queue List*/ topicQueueList;

    private ConcurrentHashMap<String/*Topic*/, ReadWriteLock>/*Queue List Lock*/ topicQueueLock;


    @Override
    public boolean load() {
        return false;
    }

    @Override
    public void start() throws Exception {
        if (!start) {
            this.topicQueueList = new ConcurrentHashMap<>();
            this.topicQueueLock = new ConcurrentHashMap<>();
            start = true;
        }
    }

    @Override
    public void shutdown() {
        shutdown = true;
    }

    @Override
    public void destroy() {
        this.topicQueueList = null;
    }

    @Override
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        return PutMessage(msg);
    }

    private PutMessageResult PutMessage(MessageExt msg) {
        if (!shutdown) {
            String topic = msg.getTopic();
            if (!topicQueueList.containsKey(topic)) {
                topicQueueList.put(topic, new ArrayList<LinkedBlockingQueue<Message>>());
                topicQueueLock.put(topic, new ReentrantReadWriteLock());
            }
            int queueId = msg.getQueueId();
            if (topicQueueList.get(topic).size() <= queueId) {
                try {
                    topicQueueLock.get(topic).writeLock().lock();
                    for (int i = topicQueueList.get(topic).size(); i < queueId + 1; i++) {
                        topicQueueList.get(topic).add(new LinkedBlockingQueue<Message>());
                    }
                } finally {
                    topicQueueLock.get(topic).writeLock().unlock();
                }
            }
            try {
                topicQueueLock.get(topic).readLock().lock();
                LinkedBlockingQueue<Message> queue = topicQueueList.get(topic).get(msg.getQueueId());
                queue.offer(msg);
            } finally {
                topicQueueLock.get(topic).readLock().unlock();
            }


            return new PutMessageResult(PutMessageStatus.PUT_OK, new AppendMessageResult(AppendMessageStatus.PUT_OK));
        } else
            return new PutMessageResult(PutMessageStatus.SERVICE_NOT_AVAILABLE, new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR));
    }

    @Override
    public PutMessageResult putMessages(MessageExtBatch messageExtBatch) {
        return PutMessage(messageExtBatch);
    }

    @Override
    public GetMessageResult getMessage(String group, String topic, int queueId, long offset, int maxMsgNums, MessageFilter messageFilter) {
        if (!shutdown) {
            GetMessageResult result = new GetMessageResult();
            if (!topicQueueList.containsKey(topic)) {
                result = new GetMessageResult();
                result.setStatus(GetMessageStatus.NO_MATCHED_MESSAGE);
                return result;
            }
            try {
                topicQueueLock.get(topic).readLock().lock();
                List<ByteBuffer> messages = new ArrayList<>();
                int count = Math.min(maxMsgNums, topicQueueList.get(topic).get(queueId).size());
                for (int i = 0; i < count; i++) {
                    if (topicQueueList.get(topic).get(queueId).remove().getBody() == null) {
                        messages.add(ByteBuffer.allocate(0));
                    } else
                        messages.add(ByteBuffer.wrap(topicQueueList.get(topic).get(queueId).remove().getBody()));
                }
                for (ByteBuffer byteBuffer : messages) {
                    SelectMappedBufferResult selectMappedBufferResult =
                            new SelectMappedBufferResult(0, byteBuffer, byteBuffer.capacity(), null);
                    result.addMessage(selectMappedBufferResult);
                }
                result.setBufferTotalSize(count);
                result.setStatus(GetMessageStatus.FOUND);
                result.setNextBeginOffset(0);
            } finally {
                topicQueueLock.get(topic).readLock().unlock();
            }
            return result;
        } else {
            GetMessageResult result = new GetMessageResult();
            result.setStatus(GetMessageStatus.NO_MATCHED_MESSAGE);
            return result;
        }
    }

    @Override
    public long getMaxOffsetInQueue(String topic, int queueId) {
        if (shutdown || !topicQueueList.containsKey(topic)) {
            return 0;
        }
        try {
            topicQueueLock.get(topic).readLock().lock();
            return topicQueueList.get(topic).get(queueId).size();
        } finally {
            topicQueueLock.get(topic).readLock().unlock();
        }
    }

    @Override
    public long getMinOffsetInQueue(String topic, int queueId) {
        return 0;
    }

    @Override
    public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
        return 0;
    }

    @Override
    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        return 0;
    }

    @Override
    public MessageExt lookMessageByOffset(long commitLogOffset) {
        return null;
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset) {
        return null;
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
        return null;
    }

    @Override
    public String getRunningDataInfo() {
        return null;
    }

    @Override
    public HashMap<String, String> getRuntimeInfo() {
        return null;
    }

    @Override
    public long getMaxPhyOffset() {
        return 0;
    }

    @Override
    public long getMinPhyOffset() {
        return 0;
    }

    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        if (!topicQueueList.containsKey(topic) || queueId >= topicQueueList.get(topic).size()) {
            return 0;
        }
        return ((MessageExtBrokerInner) topicQueueList.get(topic).get(queueId).peek()).getBornTimestamp();
    }

    @Override
    public long getEarliestMessageTime() {
        long min = Long.MAX_VALUE;

        for (Map.Entry<String, List<LinkedBlockingQueue<Message>>> stringListEntry : topicQueueList.entrySet()) {
            for (LinkedBlockingQueue<Message> messages : stringListEntry.getValue()) {
                Math.min(min, ((MessageExtBrokerInner) messages.peek()).getBornTimestamp());
            }
        }
        return min;
    }

    @Override
    public long getMessageStoreTimeStamp(String topic, int queueId, long consumeQueueOffset) {
        return getEarliestMessageTime(topic, queueId);
    }

    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {
        if (!topicQueueList.containsKey(topic) || queueId >= topicQueueList.get(topic).size()) {
            return 0;
        }
        return topicQueueList.get(topic).get(queueId).size();
    }

    @Override
    public SelectMappedBufferResult getCommitLogData(long offset) {
        return null;
    }

    @Override
    public boolean appendToCommitLog(long startOffset, byte[] data) {
        return false;
    }

    @Override
    public void executeDeleteFilesManually() {

    }

    @Override
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end) {
        return null;
    }

    @Override
    public void updateHaMasterAddress(String newAddr) {

    }

    @Override
    public long slaveFallBehindMuch() {
        return 0;
    }

    @Override
    public long now() {
        return 0;
    }

    @Override
    public int cleanUnusedTopic(Set<String> topics) {
        return 0;
    }

    @Override
    public void cleanExpiredConsumerQueue() {

    }

    @Override
    public boolean checkInDiskByConsumeOffset(String topic, int queueId, long consumeOffset) {
        return false;
    }

    @Override
    public long dispatchBehindBytes() {
        return 0;
    }

    @Override
    public long flush() {
        return 0;
    }

    @Override
    public boolean resetWriteOffset(long phyOffset) {
        return false;
    }

    @Override
    public long getConfirmOffset() {
        return 0;
    }

    @Override
    public void setConfirmOffset(long phyOffset) {

    }

    @Override
    public boolean isOSPageCacheBusy() {
        return false;
    }

    @Override
    public long lockTimeMills() {
        return 0;
    }

    @Override
    public boolean isTransientStorePoolDeficient() {
        return false;
    }

    @Override
    public LinkedList<CommitLogDispatcher> getDispatcherList() {
        return null;
    }

    @Override
    public ConsumeQueue getConsumeQueue(String topic, int queueId) {
        return null;
    }
}
