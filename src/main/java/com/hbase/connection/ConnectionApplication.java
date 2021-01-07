package com.hbase.connection;

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









//		String uriAPI1 = "http://47.105.145.26:20550/ns1:t1/fakerow";
//	       String result1 = "";
//	       HttpPut httpRequst1 = new HttpPut(uriAPI1);
//	       try {
//	           httpRequst1.setHeader("accept", "application/json");
//	           httpRequst1.setHeader("Content-Type","application/json");
//	           Map<String, Object> m = new HashMap<String, Object>();
//	           m.put("column",new String(encoder.encode("cf:name".getBytes())));
//	           m.put("$",new String(encoder.encode( "王五".getBytes())));
//	           Map<String, Object> cm = new HashMap<String, Object>();
//	           cm.put("column",new String(encoder.encode("cf:sex".getBytes())));
//	           cm.put("$", new String(encoder.encode("女".getBytes())));
//	           Map<String, Object> am = new HashMap<String, Object>();
//	           am.put("column",new String(encoder.encode("cf:age".getBytes())));
//	           am.put("$", new String(encoder.encode("18".getBytes())));
//	           List<Object> list=new ArrayList<Object>();
//	           list.add(m);
//	           list.add(cm);
//	           list.add(am);
//	           Map<String,Object> m1=new HashMap<String,Object>();
//	           m1.put("Cell", list);
//	           m1.put("key", new String(encoder.encode("3".getBytes())));
//	           List<Object> list1=new ArrayList<Object>();
//	           list1.add(m1);
//	           Map<String,Object> m2=new HashMap<String,Object>();
//	           m2.put("Row",list1);
//
//	           ObjectMapper mapper = new ObjectMapper();
//	           mapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, false);
//	           String value =mapper.writeValueAsString(m2);
//	           StringEntity s =null;
//	           s = new StringEntity(value,"UTF-8");
//	           httpRequst1.setEntity(s);
//	           HttpResponse httpResponse = new DefaultHttpClient().execute(httpRequst1);
//	           // 其中HttpGet是HttpUriRequst的子类
//	           int statusCode = httpResponse.getStatusLine().getStatusCode();
//	           if (statusCode == 200||statusCode == 201|| statusCode == 403) {
//	               HttpEntity httpEntity = httpResponse.getEntity();
//	               result1= "code=>"+statusCode+":"+EntityUtils.toString(httpEntity);// 取出应答字符串
//	               // 一般来说都要删除多余的字符
//	               // 去掉返回结果中的"\r"字符，否则会在结果字符串后面显示一个小方格
//	               // result.replaceAll("\r", "");
//	           } else {
//	               result1= "没有返回正确的状态码，请仔细检查请求表名";
//	        	   httpRequst1.abort();
//	               throw new RuntimeException("Failed : HTTP error code : " + statusCode);
//	           }
//	       }catch(Exception e) {
//	           e.printStackTrace();
//	           result1 = e.getMessage().toString();
// 	       }
//	       System.err.println(result1);
//
//
//
//
//
//
//
//
//
//
//
//
//


		String uriAPI = "http://47.105.145.26:20550/ns1:t1/*?";
		String result = "";
		// 其中HttpGet是HttpUriRequst的子类
		HttpGet httpRequst = new HttpGet(uriAPI);
		httpRequst.setHeader("accept", "text/xml");
//		httpRequst.setHeader("accept","text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
		CloseableHttpClient httpclient = HttpClients.createDefault();
		CloseableHttpResponse response1 = httpclient.execute(httpRequst);
		try {
			int statusCode = response1.getStatusLine().getStatusCode();
			if (statusCode == 200 || statusCode == 403) {
				org.apache.http.HttpEntity entity1 = response1.getEntity();
				result = EntityUtils.toString(entity1, "UTF-8");// 取出应答字符串
				// 一般来说都要删除多余的字符
				// 去掉返回结果中的"\r"字符，否则会在结果字符串后面显示一个小方格
				// result.replaceAll("\r", "");
			} else {
				// httpRequst.abort();
				result = "异常的返回码:" + statusCode;
			}
		} catch (ClientProtocolException e) {
			e.printStackTrace();
			result = e.getMessage().toString();
		} catch (IOException e) {
			e.printStackTrace();
			result = e.getMessage().toString();
		} finally {
			response1.close();

		}
		JSONObject jsonObject1 = XML.toJSONObject(result).getJSONObject("CellSet");
		System.err.println(jsonObject1);
		Object json = new JSONTokener(jsonObject1.get("Row").toString()).nextValue();
		if(json instanceof JSONObject){
			JSONObject row = jsonObject1.getJSONObject("Row");
			JSONArray cell = row.getJSONArray("Cell");
			String a=row.getString("key");
			System.out.println(new String(decoder.decode(a)));
			for (int i = 0; i < cell.length(); i++) {
				JSONObject jsonObject = new JSONObject(cell.get(i).toString());
				System.out.println(new String(decoder.decode(jsonObject.getString("column")), "UTF-8") + "--"
						+ new String(decoder.decode(jsonObject.getString("content")), "UTF-8"));
			}
		}else if (json instanceof JSONArray){
			JSONArray jsonArray = (JSONArray)json;
			for(Object arr:jsonArray){
				System.out.println(new String(decoder.decode(((JSONObject)arr).getString("key"))));
				JSONArray cell = ((JSONObject)arr).getJSONArray("Cell");
				for (int i = 0; i < cell.length(); i++) {
					JSONObject jsonObject = new JSONObject(cell.get(i).toString());
					System.out.println(new String(decoder.decode(jsonObject.getString("column")), "UTF-8") + "--"
							+ new String(decoder.decode(jsonObject.getString("content")), "UTF-8"));
				}
			}
		}

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



		conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", ZKconnect);
		ConnectionApplication a =new ConnectionApplication();
		//  String tableName="ns1:t1";
		// String[] family={"cf"};
		try {
//           HTableDescriptor htds =new HTableDescriptor(tableName);
//           for(int z=0;z<family.length;z++){
//               HColumnDescriptor h=new HColumnDescriptor(family[z]);
//               htds.addFamily(h);
//           }


			Result result1 = a.getResult("ns1:t1", "1");
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

