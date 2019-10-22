package com.sf.yarn.util;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/**
 * Created by IntelliJ IDEA. com.sf.yarn.util
 *
 * @author: 01377851
 * @Date: 2018/10/23
 * @create: 7:56 PM
 */
public class XmlUtilsTest {

  @Test
  public void getDocument() throws Exception {
  }

  @Test
  public void addNode() throws Exception {
  }

  @Test
  public void saveDocument() throws Exception {
  }

  @Test
  public void setAclSubmit() throws Exception {
    XmlUtils xmlUtils = new XmlUtils();

    List<String> name = new ArrayList<>();
    name.add("queue_a");
    name.add("queue_b");
    xmlUtils.setAclSubmit("/Users/zdb/Desktop/fair-allocation.xml",name);
  }

}