package com.sf.yarn.util;

import com.sf.yarn.api.QueueParam;
import com.sf.yarn.service.YarnApiHandler.Label;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by IntelliJ IDEA. com.sf.yarn.util
 *
 * @author: 01377851
 * @Date: 2018/10/22
 * @create: 3:02 PM
 */
public class XmlUtils {

    private static final Logger logger = LoggerFactory.getLogger(XmlUtils.class.getName());

    public Document getDocument(File file) {
        Document doc = null;
        SAXReader saxReader = new SAXReader();
        try {
            doc = saxReader.read(file);
        } catch (DocumentException e) {
            logger.error(e.getMessage(), e);
        }

        return doc;
    }

    /**
     * 对指定的节点添加子节点和对象的文本内容
     */
    public void addNode(Element node, String nodeName, String attributeName, String content) {

        Element newNode = node.addElement(nodeName);
        //对新增的节点添加文本内容content
        newNode.addAttribute("name", attributeName);
        if (content != null) {
            newNode.setText(content);
        }

    }

    /**
     * 把domcument对象保存到指定的xml文件中
     *
     */
    public void saveDocument(Document document, File xmlFile) throws IOException {
        //创建输出流
        Writer osWrite = new OutputStreamWriter(new FileOutputStream(xmlFile));
        //获取输出的指定格式
        OutputFormat format = OutputFormat.createPrettyPrint();

        format.setEncoding("UTF-8");
        //XMLWriter 指定输出文件以及格式
        XMLWriter writer = new XMLWriter(osWrite, format);
        writer.write(document);
        writer.flush();
        writer.close();
    }

    /**
     * 删除队列配置前，需要先把队列置为不可用，然后再删除队列信息
     */
    public boolean setAclSubmit(String xmlPath, List<String> queueName) {
        boolean success = false;
        XmlUtils xmlUtils = new XmlUtils();
        File xmlFile = new File(xmlPath);
        Document document = null;
        document = xmlUtils.getDocument(xmlFile);
        Element root = document.getRootElement();
        Element parent = root.element("queue");
        for (String param :
                queueName) {
            //queue级别
            List<Element> list = parent.elements("queue");
            for (Element element :
                    list) {
                if (element.attribute(0).getValue().equals(param)) {
                    //配置属性级别
                    element.element(Label.aclSubmitApps.toString()).setText("01377851");
                }
            }
        }
        try {
            xmlUtils.saveDocument(document, xmlFile);
            success = true;
        } catch (IOException e) {
            logger.error("Save Modify fail:" + e.getMessage(), e);
            return success;
        }
        return success;
    }


    public static void main(String[] args) {
        XmlUtils xmlUtils = new XmlUtils();

        File xmlFile = new File("/Users/zdb/Desktop/fair-allocation.xml");
        Document document = null;
        document = xmlUtils.getDocument(xmlFile);
        Element root = document.getRootElement();//获取根节点

        QueueParam param = new QueueParam();
        param.setQueueName("queue_d");
        param.setMinResources("2048 mb,4 vcores");
        param.setMaxResources("102400 mb,148 vcores");

        Element parent = root.element("queue");

        //新增queue父节点
        xmlUtils.addNode(parent, "queue", param.queueName, null);
        List<Element> list = parent.elements("queue");
        for (Element element :
                list) {
            if (element.attribute(0).getValue().equals(param.queueName)) {
                xmlUtils.addNode(element, "maxResources", null, param.getMaxResources());
                xmlUtils.addNode(element, "minResources", null, param.getMinResources());
                xmlUtils.addNode(element, "weight", null, "2.0");
                xmlUtils.addNode(element, "schedulingMode", null, "fair");
            }
        }

        try {
            xmlUtils.saveDocument(document, xmlFile);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }


    }


}
