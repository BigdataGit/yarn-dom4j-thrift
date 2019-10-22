package com.sf.yarn.util;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PropertyUtil {

    private static final Logger logger = LoggerFactory.getLogger(PropertyUtil.class);

    private PropertyUtil() {
    }

    /**
     * propPath：默认加载resource文件夹下的文件名,多层目录写相对路径
     */
    public static Properties getProperty(String propPath, Class<?> clazz) {
        Properties prop = new Properties();
        InputStream in = null;
        try {
            in = new BufferedInputStream(clazz.getClassLoader().getResource(propPath).openStream());
            prop.load(new InputStreamReader(in, "UTF-8"));
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        return prop;
    }

}
