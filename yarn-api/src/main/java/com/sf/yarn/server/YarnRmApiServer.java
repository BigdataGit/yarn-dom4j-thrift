package com.sf.yarn.server;

import com.sf.yarn.service.YarnApiHandler;
import com.sf.yarn.service.YarnApiService;
import com.sf.yarn.util.PropertyUtil;
import java.util.Properties;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by IntelliJ IDEA. com.sf.yarn.server
 *
 * @author: 01377851
 * @Date: 2018/10/23
 * @create: 6:10 PM
 */
public class YarnRmApiServer {

    private static final Logger logger = LoggerFactory.getLogger(YarnApiService.class.getName());
    public static final int PORT = 9090;
    private static String IPS;

    public static void main(String[] args) {

        try {
            TServerSocket serverTransport = new TServerSocket(PORT);
            TCompactProtocol.Factory proFactory = new TCompactProtocol.Factory();
            TProcessor processor = new YarnApiService.Processor(new YarnApiHandler());
            TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport);
            serverArgs.processor(processor);
            serverArgs.protocolFactory(proFactory);
            TServer server = new TThreadPoolServer(serverArgs);
            server.setServerEventHandler(new YarnApiServerEventHandler());
            server.serve();
        } catch (TTransportException e) {
            logger.error("服务启动失败：" + e.getMessage(), e);
        }

    }


    static class YarnApiServerEventHandler implements TServerEventHandler {

        /**
         * 服务成功启动后执行
         */
        @Override
        public void preServe() {
            logger.info("Start server on port:" + YarnRmApiServer.PORT + " ...");
        }

        /**
         * 当client创建连接的时候触发，只会执行一次
         */
        @Override
        public ServerContext createContext(TProtocol input, TProtocol output) {
            logger.info("开始验证用户是否拥有请求权限···");
            Properties prop = PropertyUtil.getProperty("config.properties", YarnRmApiServer.class);
            IPS = prop.getProperty("client.ip");
            TSocket socket = (TSocket) input.getTransport();
            String clientHost = socket.getSocket().getInetAddress().getHostAddress();
            logger.info("createContext ... " + clientHost);
            String[] ips = IPS.split(",");
            for (int i = 0; i < ips.length; i++) {
                if (clientHost.equals(ips[i])) {
                    logger.info("验证通过，用户ip:" + clientHost);
                    break;
                } else {
                    if (i == ips.length - 1) {
                        logger.info("客户端无请求权限！" + clientHost);
                        input.getTransport().close();
                        logger.info("客户端连接已关闭！");
                    }
                }
            }
            return null;
        }

        /**
         * 当client结束请求处理的时候触发，只会执行一次
         */
        @Override
        public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
            logger.info("deleteContext ... ");
        }

        /**
         * 调用RPC服务的时候触发 每调用一次方法，就会触发一次
         */
        @Override
        public void processContext(ServerContext serverContext, TTransport inputTransport, TTransport outputTransport) {
            /**
             * 把TTransport对象转换成TSocket，在TSocket里面获取Socket，就可以拿到客户端IP
             */
//      logger.info("开始验证用户是否拥有请求权限···");
//      Properties prop = PropertyUtil.getProperty("config.properties", YarnRmApiServer.class);
//      IPS = prop.getProperty("client.ip");
//      TSocket socket = (TSocket) inputTransport;
//      String clientInfo = socket.getSocket().getInetAddress().toString();
//      logger.info("clientip:" + clientInfo);
//      String[] ips = IPS.split(",");
//      String clientIp = clientInfo.substring(1);
//      for (int i = 0; i < ips.length; i++) {
//        if (clientIp.equals(ips[i])) {
//          logger.info("验证通过，用户ip:" + clientIp);
//          break;
//        } else {
//          if (i == ips.length - 1) {
//            logger.info("客户端无请求权限！" + clientIp);
//            inputTransport.close();
//            logger.info("客户端连接已关闭！");
//          }
//        }
//      }
        }
    }

}
