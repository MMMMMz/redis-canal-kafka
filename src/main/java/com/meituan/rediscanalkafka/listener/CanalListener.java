package com.meituan.rediscanalkafka.listener;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.meituan.rediscanalkafka.mq.MyEventPublisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author mazhe
 * @date 2023/8/24 20:05
 */
@Component
@Slf4j
public class CanalListener implements ApplicationRunner {
    @Autowired
    private MyEventPublisher eventPublisher;

    private static final int batchSize = 1;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        // 创建canal连接器
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("127.0.0.1",
                11111), "example", "canal", "canal");
        try {
            // 连接canal服务端
            connector.connect();
            // 只订阅*order的表，订阅所有表：".*\\..*"
            connector.subscribe(".*user.*");
            // 回滚到未进行ack确认的地方，下次fetch的时候，可以从最后一个没有ack的地方开始拿
            connector.rollback();
            while (true) {
                // 获取指定数量的数据
                Message message = connector.getWithoutAck(batchSize);
                // 获取批量ID
                long batchId = message.getId();
                // 获取批量的数量
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    try {
                        //如果没有数据,线程休眠2秒
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                    }
                } else {
                    //如果有数据,处理数据
                    handle(message.getEntries());
                }
                // ack确认batchId。小于等于这个batchId的消息都会被确认
                connector.ack(batchId);
            }
        } finally {
            // 释放连接
            connector.disconnect();
        }
    }

    private void handle(List<CanalEntry.Entry> entries) {
        entries.stream().forEach(entry ->{
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                // 开启/关闭事务的实体类型，跳过
                return;
            }
            if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                try {
                    // 获取rowChange
                    CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                    // 针对新增操作的监听
                    if (rowChange.getEventType() == CanalEntry.EventType.INSERT) {
                        // 遍历rowChange里的所有的行数据
                        log.info("rowChange " + JSONObject.toJSONString(rowChange));
                        rowChange.getRowDatasList().stream().forEach((row->{
                                publishEvent(row);
                        }));
                    }
                }catch (Exception e) {
                    throw new RuntimeException("解释Binlog日志出现异常:" + entry, e);
                }
            }
        });
    }

    private void publishEvent(CanalEntry.RowData rowData) {
        List<String> result = new ArrayList<>();
        List<CanalEntry.Column> columns = rowData.getAfterColumnsList();
        log.info("columns.size " + columns.size());
        log.info("columns " + JSONObject.toJSONString(columns));
        columns.forEach((column -> {
            String name = column.getName();
            String value = column.getValue();
            log.info("name:{}, value:{}", name, value);
            result.add(name + value);
        }));
        eventPublisher.publishEvent(JSONObject.toJSONString(result));
    }
}