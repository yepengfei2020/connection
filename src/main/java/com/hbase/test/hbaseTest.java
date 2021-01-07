package com.hbase.test;

import com.hbase.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;

/**
 * @program: connection
 * @description: test
 * @author: YePengFei
 * @create: 2020-10-23 10:05
 **/
public class hbaseTest {

    public static void main(String[] args) {
        String [] p = {"id","name","age"};
        // hbase 创建表
//        System.out.println("创建表");
//        new HBaseUtil().createTable("people",p);
//        System.out.println("创建表完成,开始put数据");

          new HBaseUtil().getRow("hbase_1102","001");
//        byte[] row = Bytes.toBytes(rows);
//        Put put = new Put(row);
//        put.add(Bytes.toBytes(rowkey),Bytes.toBytes(columnName1),Bytes.toBytes(columnValue1));
//        put.add(Bytes.toBytes(rowkey),Bytes.toBytes(columnName2),Bytes.toBytes(columnValue2));
//        put.add(Bytes.toBytes(rowkey),Bytes.toBytes(columnName3),Bytes.toBytes(columnValue3));
//        new HBaseUtil().putRows("people")

    }


    public static void  test() throws IOException {

//        Configuration configuration = HBaseConfiguration.create();
//        Connection connection = ConnectionFactory.createConnection(configuration);
//        Admin admin = connection.getAdmin();
//
//        if(admin.tableExists(TableName.valueOf(Bytes.toBytes("hbase_1102")))) {//如果存在要创建的表，那么先删除，再创建
//            admin.disableTable(TableName.valueOf(Bytes.toBytes("hbase_1102"))));
//            admin.deleteTable(TableName.valueOf(Bytes.toBytes("hbase_1102"))));
//        }
//        HTableDescriptor tableDescriptor = new HTableDescriptor();
//
//        admin.createTable(tableDescriptor);
//        admin.disableTable(tableName);
//        HColumnDescriptor hd = new HColumnDescriptor(columnFamily);
//        admin.addColumn(tableName,hd);


    }


}
