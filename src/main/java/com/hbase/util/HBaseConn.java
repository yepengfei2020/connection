package com.hbase.util;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
/**
 * @program: connection
 * @description: HBaseConn
 * @author: YePengFei
 * @create: 2020-10-23 09:56
 **/

public class HBaseConn {
    private static final HBaseConn INSTANCE = new HBaseConn();
    private static  Configuration configuration; //hbase配置
    private static  Connection connection; //hbase connection
    private HBaseConn(){
        try{
            if (configuration==null){
                configuration = HBaseConfiguration.create();
                configuration.set("hbase.zookeeper.quorum","192.168.112.129:2181");
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static Connection getHBaseConn(){
        return INSTANCE.getConnection();
    }
    public static Table getTable(String tableName) throws IOException {
        return INSTANCE.getConnection().getTable(TableName.valueOf(Bytes.toBytes(tableName)));
    }

    public static void closeConn(){
        if (connection!=null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private  Connection getConnection(){
        if (connection==null || connection.isClosed()){
            try{
                connection = ConnectionFactory.createConnection(configuration);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return connection;
    }



}


