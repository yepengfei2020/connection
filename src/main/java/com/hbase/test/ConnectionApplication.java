package com.hbase.test;

import io.leopard.javahost.JavaHost;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.XML;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Properties;

@SpringBootApplication
public class ConnectionApplication {
	static final Base64.Decoder decoder = Base64.getDecoder();
	static final Base64.Encoder encoder= Base64.getEncoder();

	static Configuration conf =null;
	private static final String ZKconnect="47.105.145.26:2181";



	public static void main(String[] args) throws ClientProtocolException, IOException  {
		SpringApplication.run(ConnectionApplication.class, args);
		Resource resource = new ClassPathResource("/vdns.properties");
		Properties props = PropertiesLoaderUtils.loadProperties(resource);
		JavaHost.updateVirtualDns(props);
		JavaHost.printAllVirtualDns();// 打印所有虚拟DNS记录














		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "47.105.145.26");
		conf.set("hbase.zookeeper.property.client", "2181");

		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		System.out.println(admin.tableExists(TableName.valueOf("ns1:t1")));
		Table table = conn.getTable(TableName.valueOf("ns1:t1"));
		Scan scan = new Scan();
		scan.setStartRow("1".getBytes());
		System.out.println(table.toString());
		ResultScanner scanner = table.getScanner(scan);
		System.out.println("=--------" + scanner.toString());
		for (Result res : scanner) {
			if (res.raw().length == 0) {
				System.out.println("ns1:t1" + " 表数据为空！");
			} else {
				for (KeyValue kv : res.raw()) {
					System.out.println(new String(kv.getKey()) + "===" + new String(kv.getValue()));
				}
			}
		}

//
//	        String hostname = Strings.domainNamePointerToHostName(DNS.getDefaultHost(
//	                conf.get("hbase.master.dns.interface", "default"),
//	                conf.get("hbase.master.dns.nameserver", "default")));
//	        System.out.println("hostname" + hostname);
// 	        scan.setStartRow("1".getBytes());
//	        System.out.println(table.toString());
//	        ResultScanner scanner1 = table.getScanner(scan);
//	        for (Result res : scanner1) {
//	            if (res.raw().length == 0) {
//	                System.out.println("ns1:t1" + " 表数据为空！");
//	            } else {
//	                for (KeyValue kv : res.raw()) {
//	                    System.out.println(new String(kv.getKey()) + "==" + new String(kv.getValue()));
//	                }
//	            }
//	        }
//
//





//	        RestTemplate restTemplate = new RestTemplate();
//
//			HttpHeaders headers = new HttpHeaders();
//			headers.add("Atuthorization","Basic cnNfdGrsYWIzzdss190s2");
////			headers.add("Content-Type", "application/x-www-form-urlencoded");
////			headers.add("Accept", "text/xml");
//
//			List<String> cookies =new ArrayList<String>();
//			cookies.add("hadoop_auth=r=tj; Path=/; HttpOnly");       //在 header 中存入cookies
//		    headers.put(HttpHeaders.COOKIE,cookies);        //将cookie存入头部
//			MultiValueMap<String, String> map = new LinkedMultiValueMap<String, String>();      //map里面是请求体的内容
////	        map.add("Content-Type", "text/xml");
// 	        HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<MultiValueMap<String, String>>(map, headers);
//	        ResponseEntity<String> response = restTemplate.postForEntity("http://47.105.145.26:8080/remethod/all", request, String.class);
//	        System.out.println(response.getHeaders().get("Set-Cookie").get(0));
//	        System.out.println(response.getBody());


	}
	public Result getResult(String tableName, String rowKey) throws Exception {
		Get get=new Get(Bytes.toBytes(rowKey));
		HTable htable=new HTable(conf, Bytes.toBytes(tableName));
		Result result=htable.get(get);
//	      for(KeyValue k:result.list()){
//	          System.out.println(Bytes.toString(k.getFamily()));
//	          System.out.println(Bytes.toString(k.getQualifier()));
//	          System.out.println(Bytes.toString(k.getValue()));
//	          System.out.println(k.getTimestamp());
//	      }
		return result;
	}

	public static void printKeyValye(KeyValue kv) {
		System.out.println(Bytes.toString(kv.getRow()) + "\t" + Bytes.toString(kv.getFamily()) + "\t" + Bytes.toString(kv.getQualifier()) + "\t" + Bytes.toString(kv.getValue()) + "\t" + kv.getTimestamp());
	}
}

