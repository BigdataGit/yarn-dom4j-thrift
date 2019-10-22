package com.sf.yarn.client;

import com.sf.yarn.api.QueueParam;
import com.sf.yarn.service.YarnApiService;
import java.util.ArrayList;
import java.util.List;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by IntelliJ IDEA. com.sf.yarn.client
 *
 * @author: 01377851
 * @Date: 2018/10/24
 * @create: 10:33 AM
 */
public class YarnRmClient {

    private static final Logger logger = LoggerFactory.getLogger(YarnRmClient.class.getName());

    public static void main(String[] args) {

        TTransport transport = new TSocket("localhost", 9090);
        TProtocol protocol = new TCompactProtocol(transport);
        try {
            transport.open();
        } catch (TTransportException e) {
            logger.error("connect to YarnRmApi Server error:" + e.getMessage(), e);
        }
        logger.info("Connected to YarnRmApi Server");
        YarnApiService.Client client = new YarnApiService.Client(protocol);
//        List<QueueParam> paramList = new ArrayList<>();
//    QueueParam param = new QueueParam();
//    param.setQueueName("queue_j");
//    param.setMinResources("2048 mb,4 vcores");
//    param.setMaxResources("102400 mb,148 vcores");
//    paramList.add(param);
        List<String> queueName = new ArrayList<>();
        queueName.add("next");
        try {
//      client.addQueueResource(paramList);
//        client.updateQueueResource(paramList);
            client.delQueueResource(queueName);
        } catch (TException e) {
            logger.error("" + e.getMessage(), e);
        }
    }
}

