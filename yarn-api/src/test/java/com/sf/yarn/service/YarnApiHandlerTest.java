package com.sf.yarn.service;

import com.sf.yarn.api.QueueParam;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/**
 * Created by IntelliJ IDEA. com.sf.yarn.service
 *
 * @author: 01377851
 * @Date: 2018/10/22
 * @create: 8:13 PM
 */
public class YarnApiHandlerTest {

  @Test
  public void addQueueResource() throws Exception {
    List<QueueParam> paras = new ArrayList<>();
    YarnApiHandler yarnApiHandler = new YarnApiHandler();
    QueueParam queueParam = new QueueParam();
    queueParam.setQueueName("queue_a");
    queueParam.setMaxResources("aaa");
    queueParam.setMinResources("bbb");
    queueParam.setMaxRunningApps("150");
    paras.add(queueParam);
    yarnApiHandler.addQueueResource(paras);
  }

  @Test
  public void updateQueueResource() throws Exception {
    List<QueueParam> paras = new ArrayList<>();
    YarnApiHandler yarnApiHandler = new YarnApiHandler();
    QueueParam queueParam = new QueueParam();
    queueParam.setQueueName("queue_a");
    queueParam.setMaxResources("aaa");
    queueParam.setMinResources("bbb");
    queueParam.setMaxRunningApps("15");
    paras.add(queueParam);
    yarnApiHandler.updateQueueResource(paras);
  }

  @Test
  public void delQueueResource() throws Exception{
    YarnApiHandler yarnApiHandler = new YarnApiHandler();
    List<String> queueName = new ArrayList<>();
    queueName.add("queue_a");
//    queueName.add("queue_d");
    yarnApiHandler.delQueueResource(queueName);
  }

}