package com.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.net.DNS;
import org.apache.htrace.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.cookie.Cookie;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import com.google.gson.Gson;

import io.leopard.javahost.JavaHost;

import org.json.XML;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@SpringBootApplication
public class ConnectionApplication {

	static Configuration conf =null;
	private static final String ZKconnect="192.168.112.129:2181";



	public static void main(String[] args) throws IOException  {
		SpringApplication.run(ConnectionApplication.class, args);
		Resource resource = new ClassPathResource("/vdns.properties");
		Properties props = PropertiesLoaderUtils.loadProperties(resource);
		JavaHost.updateVirtualDns(props);
//		JavaHost.printAllVirtualDns();// 打印所有虚拟DNS记录
		conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", ZKconnect);
		ConnectionApplication a =new ConnectionApplication();
		try {

			Result result1 = a.getResult("hbase_1102", "001");
			System.out.println("result1"+result1.toString());
			List<Cell> cells = result1.listCells();
			for (int i = 0; i < cells.size(); i++) {
				Cell cell = cells.get(i);
				System.out.println(cell.toString());
				//          printCell(cell);
			}

			List<KeyValue> list = result1.list();
			for (int i = 0; i < list.size(); i++) {
				KeyValue kv = list.get(i);
				printKeyValye(kv);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}
	public Result getResult(String tableName, String rowKey) throws Exception {
		Get get=new Get(Bytes.toBytes(rowKey));
		HTable htable=new HTable(conf, Bytes.toBytes(tableName));
		Result result=htable.get(get);
		return result;
	}

	public static void printKeyValye(KeyValue kv) {
		System.out.println(Bytes.toString(kv.getRow()) + "\t" + Bytes.toString(kv.getFamily()) + "\t" + Bytes.toString(kv.getQualifier()) + "\t" + Bytes.toString(kv.getValue()) + "\t" + kv.getTimestamp());
	}
}

