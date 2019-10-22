package com.sf.yarn.service;

import com.sf.yarn.api.QueueParam;
import com.sf.yarn.util.PropertyUtil;
import com.sf.yarn.util.XmlUtils;
import java.io.File;
import java.util.List;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.dom4j.Document;
import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by IntelliJ IDEA. com.sf.yarn.service
 *
 * @author: 01377851
 * @Date: 2018/10/22
 * @create: 2:49 PM
 */
public class YarnApiHandler implements YarnApiService.Iface {

    private static final Logger logger = LoggerFactory.getLogger(YarnApiHandler.class.getName());
    private static Properties prop = PropertyUtil.getProperty("config.properties", YarnApiHandler.class);
    private static String XML_PATH = null;
    private static String QUEUE_WEIGHT = null;
    private static String QUEUE_SCHEDULINGMODE = null;
    private static String QUEUE_ACL_SUBMITAPPS = null;


    static {
        XML_PATH = prop.getProperty("yarn.queue.path");
        QUEUE_WEIGHT = prop.getProperty("queue.weight");
        QUEUE_SCHEDULINGMODE = prop.getProperty("queue.schedulingmode");
        QUEUE_ACL_SUBMITAPPS = prop.getProperty("queue.aclsubmitapps");
    }

    /**
     * 添加队列资源
     */
    @Override
    public boolean addQueueResource(List<QueueParam> paras) throws TException {
        logger.info("Add queue settings beginning······");
        boolean success = false;
        XmlUtils xmlUtils = new XmlUtils();
        File xmlFile = new File(XML_PATH);
        Document document = null;
        document = xmlUtils.getDocument(xmlFile);
        Element root = document.getRootElement();
        try {
            Element parent = root.element("queue");
            for (QueueParam param :
                    paras) {
                xmlUtils.addNode(parent, "queue", param.queueName, null);
                List<Element> list = parent.elements("queue");
                for (Element element :
                        list) {
                    if (element.attribute(0).getValue().equals(param.queueName)) {
                        xmlUtils.addNode(element, Label.minResources.toString(), null, param.getMinResources());
                        xmlUtils.addNode(element, Label.maxResources.toString(), null, param.getMaxResources());
                        xmlUtils.addNode(element, Label.maxRunningApps.toString(), null, param.getMaxResources());
                        xmlUtils.addNode(element, Label.aclSubmitApps.toString(), null, QUEUE_ACL_SUBMITAPPS);
                        xmlUtils.addNode(element, Label.weight.toString(), null, QUEUE_WEIGHT);
                        xmlUtils.addNode(element, Label.schedulingMode.toString(), null, QUEUE_SCHEDULINGMODE);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Add Queue Settings fail：" + e.getMessage(), e);
            return success;
        }

        try {
            xmlUtils.saveDocument(document, xmlFile);
            success = true;
            logger.info("Add queue settings success");
            return success;
        } catch (Exception e) {
            logger.error("Add Modify fail:" + e.getMessage(), e);
            return success;
        }
    }

    /**
     * 更新队列资源
     */
    @Override
    public boolean updateQueueResource(List<QueueParam> paras) throws TException {
        logger.info("Update queue settings beginning······");
        boolean success = false;
        XmlUtils xmlUtils = new XmlUtils();
        File xmlFile = new File(XML_PATH);
        Document document = null;
        document = xmlUtils.getDocument(xmlFile);
        Element root = document.getRootElement();
        Element parent = root.element("queue");
        for (QueueParam param :
                paras) {
            //queue级别
            List<Element> list = parent.elements("queue");
            for (Element element :
                    list) {
                if (element.attribute(0).getValue().equals(param.queueName)) {
                    //配置属性级别
                    if (param.maxResources != null && !"".equals(param.maxResources)) {
                        element.element(Label.maxResources.toString()).setText(param.maxResources);
                    }
                    if (param.minResources != null && !"".equals(param.minResources)) {
                        element.element(Label.minResources.toString()).setText(param.minResources);
                    }
                    if (param.maxRunningApps != null && !"".equals(param.maxRunningApps)) {
                        element.element(Label.maxRunningApps.toString()).setText(param.maxRunningApps);
                    }
                }
            }
        }
        try {
            xmlUtils.saveDocument(document, xmlFile);
            success = true;
            logger.info("Update queue settings success");
            return success;
        } catch (Exception e) {
            logger.error("Update Modify fail:" + e.getMessage(), e);
            return success;
        }

    }

    /**
     * 删除队列资源
     */
    @Override
    public boolean delQueueResource(List<String> queueName) throws TException {
        logger.info("Delete queue settings beginning······");
        boolean success = false;
        XmlUtils xmlUtils = new XmlUtils();
        //删除配置前需要先把队列停用，然后执行删除配置
        xmlUtils.setAclSubmit(XML_PATH, queueName);
//        //配置结束，等待RM自动刷新配置生效，10s
//        try {
//            logger.info("等待RM自动刷新，更改配置生效···");
//            Thread.sleep(10000);
//        } catch (InterruptedException e) {
//            logger.error(e.getMessage(), e);
//        }
        File xmlFile = new File(XML_PATH);
        Document document = null;
        try {
            document = xmlUtils.getDocument(xmlFile);
            Element root = document.getRootElement();
            for (String param :
                    queueName) {
                Element parent = root.element("queue");
                if (!StringUtils.isEmpty(param)) {
                    List<Element> elementList = parent.elements("queue");
                    for (Element element :
                            elementList) {
                        if (element.attribute(0).getValue().equals(param)) {
                            parent.remove(element);
                        }
                    }

                }
            }
            logger.info("Delete queue settings success");
        } catch (Exception e) {
            logger.error("Delete queue setting fail:" + e.getMessage(), e);
            return success;
        }
        try {
            xmlUtils.saveDocument(document, xmlFile);
            success = true;
        } catch (Exception e) {
            logger.error("Save Modify fail:" + e.getMessage(), e);
        }
        return success;
    }


    public enum Label {
        maxResources, minResources, maxRunningApps, weight, schedulingMode, aclSubmitApps

    }
}
