package com.hbase.util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @program: connection
 * @description: hbase_Java_Api
 * @author: YePengFei
 * @create: 2020-10-21 10:02
 **/
public class javaapi {
    protected  static Connection conn;
    private static final String ZK_QUORUM = "hbase.zookeeper.quorum";
    private static final String ZK_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
    private static final String HBASE_POS = "192.168.112.129";
    private static final String ZK_POS = "192.168.112.129:2181";
    private static final String ZK_PORT_VALUE = "2181";
    private static final Configuration conf;

    /*** 静态构造，在调用静态方法前运行，  初始化连接对象  * */
    static {
        conf = HBaseConfiguration.create();
//        conf.set("hbase.rootdir", "hdfs://"+ HBASE_POS + ":9000/hbase");
        conf.set(ZK_QUORUM, ZK_POS);
//        conf.set(ZK_CLIENT_PORT, ZK_PORT_VALUE);
        //创建连接池
        try {
            conn = ConnectionFactory.createConnection(conf);
            System.out.println(conn);
            if(conn !=null){
                System.out.println("连接成功！！！");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws IOException {
        System.out.println("执行main方法");
        getRow("hbase_1102","001");
//        createTable("hbase","info","001","id",
//                "001","name","zhangsan","age","19");
    }

    // 创建表
    public static  void createTable(String tableName,String rows, String rowkey ,String columnName1,String columnValue1,String columnName2,String columnValue2,String columnName3,String columnValue3) throws IOException{
        HTable table = new HTable(conf, tableName);
        byte[] row = Bytes.toBytes(rows);
        Put put = new Put(row);
        put.add(Bytes.toBytes(rowkey),Bytes.toBytes(columnName1),Bytes.toBytes(columnValue1));
        put.add(Bytes.toBytes(rowkey),Bytes.toBytes(columnName2),Bytes.toBytes(columnValue2));
        put.add(Bytes.toBytes(rowkey),Bytes.toBytes(columnName3),Bytes.toBytes(columnValue3));
        table.put(put);
        table.close();
    }

    // 获取表数据

    public static void getRow(String tableName, String rowKey) throws IOException {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));   //  通过rowkey创建一个 get 对象
        Result result = table.get(get);         //  输出结果
        System.out.println("打印数据结束");
        System.out.println(result);
        System.out.println("开始打印数据");
        for (Cell cell : result.rawCells()) {

            System.out.println(
                    "\u884c\u952e:" + new String(CellUtil.cloneRow(cell)) + "\t" +
                            "\u5217\u65cf:" + new String(CellUtil.cloneFamily(cell)) + "\t" +
                            "\u5217\u540d:" + new String(CellUtil.cloneQualifier(cell)) + "\t" +
                            "\u503c:" + new String(CellUtil.cloneValue(cell)) + "\t" +
                            "\u65f6\u95f4\u6233:" + cell.getTimestamp());

        }
        table.close();
        conn.close();
        conf.clear();
    }




}
