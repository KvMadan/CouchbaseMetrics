/**
 * 
 */
package in.km.oneview.cb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

/**
 * @author Madan Kavarthapu
 * 
 */
public class Main {

	final static Logger log = Logger.getLogger(Main.class);
	
	private String hostname = "";
	private String port = "";
	private int frequency;
	private String testName;
	private String dbName;
	
	private final ScheduledExecutorService service;
	
	private GenericMysqlMetricsSender mysqlMetricsSender;
	
	private InfluxSender influxSender;

	private String CB_SERVER_STATS_URI = "pools/default";
	private String CB_SERVER_BUCKET_URI = "pools/default/buckets";

	private String CB_SERVER_USERNAME = "";
	private String CB_SERVER_PASSWORD = "";
	
	private String CB_URL = "http://%s:%s/"+CB_SERVER_STATS_URI;
	private String CB_BUCKET_URL = "http://%s:%s/" + CB_SERVER_BUCKET_URI + "/" + "%s/";
	private String CB_BUCKETS_URL = "http://%s:%s/" + CB_SERVER_BUCKET_URI + "/";


	/*
	 * Couchbase Metrics URLs used in the below Logic
	 * http://indlvlincbd03:8091/pools/default
	 * http://indlvlincbd03:8091/pools/default/buckets/
	 * 
	 * Python based Script: https://www.site24x7.com/plugins/couchbase-monitoring.html
	 */
	
	public Main(String hostname, String port, String frequency, String dbName){
		try {
			this.hostname = hostname;
			this.port = port;
			this.dbName = dbName;
			
			CB_URL = String.format(CB_URL, hostname, port);
			CB_BUCKETS_URL = String.format(CB_BUCKETS_URL, hostname, port);
			
			if (frequency != null && !frequency.isEmpty())
				this.frequency = Integer.parseInt(frequency);
			else
				this.frequency = 5;
			if (testName != null && !testName.isEmpty())
				this.testName = testName + "_" + new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date());
			else
				this.testName = "Test_" + new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date());
		} catch (Exception e) {
			log.error("Exception", e);
			e.printStackTrace();
			System.exit(0);
		}
		service = Executors.newScheduledThreadPool(5);
	}
	
	public void getClusterMetrics(){
		Runnable reportSenderThread = new Runnable(){
			public void run() {
				try {
					Thread.currentThread().setName("CB Cluster Metrics Retriever");
					//System.out.println("Get Metrics");
					
					URL url = new URL(CB_URL);
					log.debug(CB_URL);
					
					HttpURLConnection conn = (HttpURLConnection) url.openConnection();
					conn.setRequestMethod("GET");
					conn.setRequestProperty("Accept", "application/json");
					
					if (conn.getResponseCode() != 200) {
						throw new RuntimeException("Failed : HTTP error code : "
								+ conn.getResponseCode());
					}
					
					BufferedReader br = new BufferedReader(new InputStreamReader(
							(conn.getInputStream())));

					StringBuffer output = new StringBuffer();
					//System.out.println("Output from Server .... \n");
					String line = "";
					while ((line = br.readLine()) != null) {
						output.append(line);
					}
					//System.out.println(output.toString());
					
					JSONParser parse = new JSONParser();
					
					JSONObject jobj = (JSONObject)parse.parse(output.toString());
					//System.out.println("Retrieve Storage Totals");
					JSONObject storageDetails = (JSONObject) jobj.get("storageTotals");
					//System.out.println("Storage Details: " + storageDetails.toString());
					JSONObject ramDetails = (JSONObject)storageDetails.get("ram");
					//System.out.println("RAM Details: "+ ramDetails);
					
					/*
					System.out.println("RAM Used: " + ramDetails.get("used"));
					System.out.println("RAM Total: " + ramDetails.get("total"));
					System.out.println("RAM Used for Data: " + ramDetails.get("usedByData"));
					*/
					JSONObject hddDetails = (JSONObject)storageDetails.get("hdd");
					log.debug("HDD Details: "+ hddDetails);
					/*
					System.out.println("HDD Used: " + hddDetails.get("used"));
					System.out.println("HDD Total: " + hddDetails.get("total"));
					System.out.println("HDD Used for Data: " + hddDetails.get("usedByData"));
					System.out.println("HDD Free: " + hddDetails.get("free"));
					*/
					
					HashMap<String, Object> map = new HashMap<String, Object>();
					//RAM
					map.put("CL_RAM_USED", convertBytesToMB(ramDetails.get("used").toString()));
					map.put("CL_RAM_TOTAL", convertBytesToMB(ramDetails.get("total").toString()));
					map.put("CL_RAM_USED_FOR_DATA", convertBytesToMB(ramDetails.get("usedByData").toString()));
					
					//HDD
					map.put("CL_HDD_USED", convertBytesToMB(hddDetails.get("used").toString()));
					map.put("CL_HDD_TOTAL", convertBytesToMB(hddDetails.get("total").toString()));
					map.put("CL_HDD_USED_FOR_DATA", convertBytesToMB(hddDetails.get("usedByData").toString()));
					map.put("CL_HDD_FREE", convertBytesToMB(hddDetails.get("free").toString()));
					
					//
					map.put("ftsMemoryQuota", jobj.get("ftsMemoryQuota").toString());
					map.put("indexMemoryQuota", jobj.get("indexMemoryQuota").toString());
					map.put("memoryQuota", jobj.get("memoryQuota").toString());
					map.put("maxBucketCount", jobj.get("maxBucketCount").toString());
					//map.put("maxBucketCount", jobj.get("maxBucketCount").toString());
					
					log.debug(jobj.get("counters").toString());
					
					JSONObject counters = (JSONObject)jobj.get("counters");
					map.put("rebalance_success", (Long) counters.get("rebalance_success"));
					map.put("rebalance_start", (Long) counters.get("rebalance_start"));
					map.put("rebalance_stop", (Long) counters.get("rebalance_stop"));
					map.put("rebalance_fail", (Long) counters.get("rebalance_fail"));
					map.put("failover_node", (Long) counters.get("failover_node"));
					
					map.put("clusterName", jobj.get("clusterName").toString());
					
					log.debug("Added Metrics to Map");
					
					java.util.Date dt = new java.util.Date();
					java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
					String currentTime = sdf.format(dt);
					log.debug("Generated TimeStamp");
					
					if(!map.isEmpty() && dbName.startsWith("mysql"))
						mysqlMetricsSender.writeClusterMetricsToDB(testName, map, currentTime);
					else if (!map.isEmpty() && dbName.startsWith("Influx")){
						influxSender.writeMetricsToDB("cluster", map, dt);
					}
					
					log.debug("Sent Metrics to DB");
					
					JSONArray nodeArray = (JSONArray)jobj.get("nodes");

					//Traverse through NodeArray.
					for (int i = 0; i < nodeArray.size(); i++) {
						
						//Get the Node Array 
						JSONObject node = (JSONObject)nodeArray.get(i);
						JSONObject systemStats = (JSONObject)node.get("systemStats");
						/*
						System.out.println("CPU Utilization Rate: [" + node.get("hostname") + "] " + systemStats.get("cpu_utilization_rate"));
						System.out.println("SWAP Total: [" + node.get("hostname") + "] " + systemStats.get("swap_total"));
						System.out.println("SWAP Used: [" + node.get("hostname") + "] " + systemStats.get("swap_used"));
						System.out.println("Mem Total: [" + node.get("hostname") + "] " + systemStats.get("mem_total"));
						System.out.println("Mem Free: [" + node.get("hostname") + "] " + systemStats.get("mem_free"));
						System.out.println("Uptime of [" + node.get("hostname") + "] " + node.get("uptime") + " Health: " + node.get("status"));
						*/
						
						map.put("ND_NAME", node.get("hostname").toString());
						map.put("ND_VERSION", node.get("version").toString());
						map.put("ND_UPTIME", node.get("uptime").toString());
						map.put("ND_Status", node.get("status").toString());
						map.put("ND_ClusterMembership", node.get("clusterMembership").toString());
						map.put("ND_CPU_USED", new Double(systemStats.get("cpu_utilization_rate").toString()));
						map.put("ND_SWAP_TOTAL", convertBytesToMB(systemStats.get("swap_total").toString()));
						map.put("ND_SWAP_USED", convertBytesToMB(systemStats.get("swap_used").toString()));
						map.put("ND_MEM_TOTAL", convertBytesToMB(systemStats.get("mem_total").toString()));
						map.put("ND_MEM_FREE", convertBytesToMB(systemStats.get("mem_free").toString()));
						
						JSONObject intStats = (JSONObject)node.get("interestingStats");
						
						if (intStats.get("couch_docs_actual_disk_size") != null)
							map.put("couch_docs_actual_disk_size", convertBytesToMB(intStats.get("couch_docs_actual_disk_size").toString()));
						if (intStats.get("couch_docs_data_size") != null)
							map.put("couch_docs_data_size", convertBytesToMB(intStats.get("couch_docs_data_size").toString()));
						if (intStats.get("couch_spatial_data_size") != null)
							map.put("couch_spatial_data_size", convertBytesToMB(intStats.get("couch_spatial_data_size").toString()));
						if (intStats.get("couch_spatial_disk_size") != null)
							map.put("couch_spatial_disk_size", convertBytesToMB(intStats.get("couch_spatial_disk_size").toString()));
						if (intStats.get("couch_views_data_size") != null)
							map.put("couch_views_data_size", convertBytesToMB(intStats.get("couch_views_data_size").toString()));
						if (intStats.get("curr_items") != null)
							map.put("curr_items", (Long)intStats.get("curr_items"));
						if (intStats.get("curr_items_tot") != null)
							map.put("curr_items_tot", (Long)intStats.get("curr_items_tot"));
						if (intStats.get("ep_bg_fetched") != null)
							map.put("ep_bg_fetched", (Long) intStats.get("ep_bg_fetched"));
						if (intStats.get("get_hits") != null)
							map.put("get_hits", new Double(intStats.get("get_hits").toString()));
						if (intStats.get("mem_used") != null)
							map.put("mem_used", convertBytesToMB(intStats.get("mem_used").toString()));
						if (intStats.get("ops") != null)
							map.put("ops", new Double(intStats.get("ops").toString()));
						if (intStats.get("vb_replica_curr_items") != null)
							map.put("vb_replica_curr_items", (Long) intStats.get("vb_replica_curr_items"));
						
						if (node.get("memoryTotal") != null)
							map.put("memoryTotal", convertBytesToMB(node.get("memoryTotal").toString()));
						if (node.get("memoryFree") != null)
							map.put("memoryFree", convertBytesToMB(node.get("memoryFree").toString()));
						if (node.get("mcdMemoryReserved") != null)
							map.put("mcdMemoryReserved", convertBytesToMB(node.get("mcdMemoryReserved").toString()));
						if (node.get("mcdMemoryAllocated") != null)
							map.put("mcdMemoryAllocated", convertBytesToMB(node.get("mcdMemoryAllocated").toString()));
						
						if(!map.isEmpty() && dbName.startsWith("mysql"))
							mysqlMetricsSender.writeNodeMetricsToDB(map, currentTime);
						else if (!map.isEmpty() && dbName.startsWith("Influx")){
							influxSender.writeMetricsToDB("node", map, dt);
						}
						
					}
					conn.disconnect();
					log.debug("Connection Disconnected");
					
				} catch (MalformedURLException e) {
					log.error("MalformedURLException", e);
					e.printStackTrace();
				} catch (ProtocolException e) {
					log.error("ProtocolException", e);
					e.printStackTrace();
				} catch (IOException e) {
					log.error("IoException", e);
					e.printStackTrace();
				} catch(ParseException e){
					log.error("ParseException", e);
					e.printStackTrace();
				} catch(Exception e){
					log.error("Exception", e);
					e.printStackTrace();
				}
			}
		};
//		 ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
		 service.scheduleAtFixedRate(reportSenderThread, 0, 5, TimeUnit.SECONDS);
	}
	
	public void getBucketMetrics(){
		Runnable reportSenderThread1 = new Runnable(){
			public void run() {
				try {
					Thread.currentThread().setName("CB Buckets Metrics Retriever");
					log.debug(CB_BUCKETS_URL);
					URL url = new URL(CB_BUCKETS_URL);
					
					HttpURLConnection conn = (HttpURLConnection) url.openConnection();
					conn.setRequestMethod("GET");
					conn.setRequestProperty("Accept", "application/json");
					
					if (conn.getResponseCode() != 200) {
						throw new RuntimeException("Failed : HTTP error code : "
								+ conn.getResponseCode());
					}
					
					BufferedReader br = new BufferedReader(new InputStreamReader(
							(conn.getInputStream())));

					StringBuffer output = new StringBuffer();
					String line = "";
					while ((line = br.readLine()) != null) {
						output.append(line);
					}
					
					JSONParser parser = new JSONParser();
					JSONArray bucketArray = (JSONArray)parser.parse(output.toString());
					log.debug("Available Buckets: " + bucketArray.size());					

					//Traverse through NodeArray.
					for (int i = 0; i < bucketArray.size(); i++) {
						
						//Get the Node Array 
						JSONObject bucket = (JSONObject)bucketArray.get(i);
						//JSONObject bucket = (JSONObject)node.get("name");
						
						log.debug("*******************" + bucket.get("name") + "*******************");
						log.debug("Bucket Type: " + bucket.get("bucketType"));
						
						JSONObject basicStats = (JSONObject) bucket.get("basicStats");
						
						log.debug("quotaPercentUsed: " + basicStats.get("quotaPercentUsed"));
						log.debug("opsPerSec: " + basicStats.get("opsPerSec"));
						log.debug("diskFetches: " + basicStats.get("diskFetches"));
						log.debug("itemCount: " + basicStats.get("itemCount"));
						log.debug("diskUsed: " + basicStats.get("diskUsed"));
						log.debug("dataUsed: " + basicStats.get("dataUsed"));
						log.debug("memUsed: " + basicStats.get("memUsed"));
						log.debug("ram " + ((JSONObject)bucket.get("quota")).get("ram").toString());
						log.debug("rawRAM " + ((JSONObject)bucket.get("quota")).get("rawRAM").toString());
						
						log.debug("*******************" + bucket.get("name") + "*******************");
						
						HashMap<String, Object> map = new HashMap<String, Object>();
						//Bucket
						log.debug("Adding to Map");
						
						if (basicStats.get("quotaPercentUsed").getClass().getSimpleName().equals("Double")){
							log.debug("Double: quotaPercentUsed : " + basicStats.get("quotaPercentUsed"));
							map.put("quotaPercentUsed", (Double) basicStats.get("quotaPercentUsed"));
						}
						else if (basicStats.get("quotaPercentUsed").getClass().getSimpleName().equals("Long")){
							log.debug("Long: quotaPercentUsed : " + basicStats.get("quotaPercentUsed"));
							map.put("quotaPercentUsed", new Float(basicStats.get("quotaPercentUsed").toString()));
						}
							
						map.put("opsPerSec", new Double(basicStats.get("opsPerSec").toString()));
						if (basicStats.get("diskFetches") != null) 
							map.put("diskFetches", new Double(basicStats.get("diskFetches").toString()));
						map.put("itemCount", new Double(basicStats.get("itemCount").toString()));
						if (basicStats.get("diskUsed") != null)
							map.put("diskUsed", convertBytesToMB(basicStats.get("diskUsed").toString()));
						if (basicStats.get("dataUsed") != null)
							map.put("dataUsed", convertBytesToMB(basicStats.get("dataUsed").toString()));
						//map.put("dataUsed", (basicStats.get("dataUsed") == null ? "" : convertBytesToMB(basicStats.get("dataUsed").toString())));
						map.put("memUsed", convertBytesToMB(basicStats.get("memUsed").toString()));
						
						map.put("ram", convertBytesToMB(((JSONObject)bucket.get("quota")).get("ram").toString()));
						map.put("rawRAM", convertBytesToMB(((JSONObject)bucket.get("quota")).get("rawRAM").toString()));
						map.put("bucketType", bucket.get("bucketType").toString());
						log.debug("Added Metrics to Map");
						
						java.util.Date dt = new java.util.Date();
						java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
						String currentTime = sdf.format(dt);
						
						//&& bucket.get("bucketType").toString().equals("membase")
						if(!map.isEmpty() && dbName.startsWith("mysql"))
							mysqlMetricsSender.writeBucketMetricsToDB(bucket.get("name").toString(), map, currentTime);
						else if (!map.isEmpty() && dbName.startsWith("Influx")){
							influxSender.writeMetricsToDB("bucket", map, dt);
						}
						
						log.debug("Sent Metrics to DB");
					}

					conn.disconnect();
					log.debug("Connection Disconnected");
					
				} catch (MalformedURLException e) {
					log.error("MalformedURLException", e);
					e.printStackTrace();
				} catch (ProtocolException e) {
					log.error("ProtocolException", e);
					e.printStackTrace();
				} catch (IOException e) {
					log.error("IOException", e);
					e.printStackTrace();
				} catch(ParseException e){
					log.error("ParseException", e);
					e.printStackTrace();
				}
				catch(Exception e){
					log.error("Exception", e);
					e.printStackTrace();
				}
			}
		};

		 //ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
		 service.scheduleAtFixedRate(reportSenderThread1, 0, 10, TimeUnit.SECONDS);
	}
	

	public void getBucketMetrics(final String bucketName){
		Runnable reportSenderThread1 = new Runnable(){
			public void run() {
				try {
					Thread.currentThread().setName("CB Bucket Metrics Retriever");
					//System.out.println("Get Metrics");
					
					CB_BUCKET_URL = String.format(CB_BUCKET_URL, bucketName);
					log.debug(CB_BUCKET_URL);
					
					URL url = new URL(CB_BUCKET_URL);
					
					HttpURLConnection conn = (HttpURLConnection) url.openConnection();
					conn.setRequestMethod("GET");
					conn.setRequestProperty("Accept", "application/json");
					
					if (conn.getResponseCode() != 200) {
						throw new RuntimeException("Failed : HTTP error code : "
								+ conn.getResponseCode());
					}
					
					BufferedReader br = new BufferedReader(new InputStreamReader(
							(conn.getInputStream())));

					StringBuffer output = new StringBuffer();
					//System.out.println("Output from Server .... \n");
					String line = "";
					while ((line = br.readLine()) != null) {
						output.append(line);
					}
					//System.out.println(output.toString());
					
					JSONParser parse = new JSONParser();
					
					JSONObject jobj = (JSONObject)parse.parse(output.toString());
					//System.out.println("Retrieve BasicStats");
					JSONObject basicStats = (JSONObject) jobj.get("basicStats");
					
					System.out.println("BucketType: " + jobj.get("bucketType").toString());
					System.out.println("quotaPercentUsed: " + basicStats.get("quotaPercentUsed"));
					System.out.println("opsPerSec: " + basicStats.get("opsPerSec"));
					System.out.println("diskFetches: " + basicStats.get("diskFetches"));
					System.out.println("itemCount: " + basicStats.get("itemCount"));
					System.out.println("diskUsed: " + basicStats.get("diskUsed"));
					System.out.println("dataUsed: " + basicStats.get("dataUsed"));
					System.out.println("memUsed: " + basicStats.get("memUsed"));
					System.out.println("ram " + ((JSONObject)jobj.get("quota")).get("ram").toString());
					System.out.println("rawRAM " + ((JSONObject)jobj.get("quota")).get("rawRAM").toString());
					//System.out.println("rawRAM " + ((JSONObject)jobj.get("bucketType")).toString());
					
					
					HashMap<String, Object> map = new HashMap<String, Object>();
					//Bucket
					log.debug("Adding to Map");
					map.put("quotaPercentUsed", basicStats.get("quotaPercentUsed").toString());
					map.put("opsPerSec", basicStats.get("opsPerSec").toString());
					map.put("diskFetches", basicStats.get("diskFetches").toString());
					map.put("itemCount", basicStats.get("itemCount").toString());
					map.put("diskUsed", convertBytesToMB(basicStats.get("diskUsed").toString()));
					map.put("dataUsed", convertBytesToMB(basicStats.get("dataUsed").toString()));
					map.put("memUsed", convertBytesToMB(basicStats.get("memUsed").toString()));
					
					map.put("ram", convertBytesToMB(((JSONObject)jobj.get("quota")).get("ram").toString()));
					map.put("rawRAM", convertBytesToMB(((JSONObject)jobj.get("quota")).get("rawRAM").toString()));
					map.put("bucketType", jobj.get("bucketType").toString());
					
					log.debug("Added Metrics to Map");
					
					java.util.Date dt = new java.util.Date();
					java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
					String currentTime = sdf.format(dt);
					log.debug("Generated TimeStamp");
					
					if(!map.isEmpty())
						mysqlMetricsSender.writeBucketMetricsToDB(bucketName, map, currentTime);
					
					log.debug("Sent Metrics to DB");
					
					conn.disconnect();
					log.debug("Connection Disconnected");
					
				} catch (MalformedURLException e) {
					log.error("MalformedURLException", e);
					e.printStackTrace();
				} catch (ProtocolException e) {
					log.error("ProtocolException", e);
					e.printStackTrace();
				} catch (IOException e) {
					log.error("IOException", e);
					e.printStackTrace();
				} catch(ParseException e){
					log.error("ParseException", e);
					e.printStackTrace();
				} catch(Exception e){
					log.error("Exception", e);
					e.printStackTrace();
				}
			}
		};

		 //ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
		 service.scheduleAtFixedRate(reportSenderThread1, 0, 5, TimeUnit.SECONDS);
	}	
	
	public Float convertBytesToMB(String bytes){
		
		//log.debug("Converting Bytes to MB");
		float byte_s = Float.parseFloat(bytes);
		float kilobytes = byte_s/1024;
		float megabytes = kilobytes/1024;
		//log.debug(bytes + "--" +  Float.toString(Math.round(megabytes)));
		//return Float.toString(Math.round(megabytes));
		return new Float(Math.round(megabytes));
	}
	
	public Integer convertObjectToInteger(String s){
		return Integer.parseInt(s);
	}
	
	public static void main(String[] args) {
		String cbserver = System.getProperty("cb.server");
		String cbport = System.getProperty("cb.port");
		String frequency = System.getProperty("cb.frequency");
		String mysqlServer = System.getProperty("mysql.server");
		String mysqlPort = System.getProperty("mysql.port");
		String mysqlDb = System.getProperty("mysql.db");
		String mysqlUser = System.getProperty("mysql.user");
		String mysqlPwd = System.getProperty("mysql.pwd");
		
		String dbName = System.getProperty("db.name");
		
		/*
		if (cbserver == null || cbport == null || 
				mysqlServer == null || mysqlPort == null ||
				mysqlDb == null || mysqlUser == null ||
				mysqlPwd == null){
			
			System.out.println("\nMissing Required parameters!\n");
			
			System.out.println("*****************************************************");
			System.out.println("Usage: ");
			System.out.println("java -Dcb.server=10.237.48.238 -Dcb.port=30010");
			System.out.println("-Dmysql.server=localhost -Dmysql.port=3306");
			System.out.println("-Dmysql.db=egdb -Dmysql.user=root -Dmysql.pwd=root");
			System.out.println("-Dcb.frequency=5 -Dcb.testname=XYZ");
			System.out.println("-jar CBEventsReader_v1.0.jar");
			System.out.println("*****************************************************");
			
			System.exit(1);
		}
		else{
			log.debug("CB Server: " + System.getProperty("cb.server"));
		}*/
		
		try {
			Main cb = new Main(cbserver, cbport, frequency, dbName);
			
			if (dbName.startsWith("mysql"))
			{
				cb.mysqlMetricsSender = new GenericMysqlMetricsSender();
				/*cb.mysqlMetricsSender.setup(System.getProperty("mysql.server"), System.getProperty("mysql.port"), 
						System.getProperty("mysql.db"), System.getProperty("mysql.user"), System.getProperty("mysql.pwd"));*/
				cb.mysqlMetricsSender.setup(mysqlServer, mysqlPort, mysqlDb, mysqlUser, mysqlPwd);
				
			}
			else if (dbName.startsWith("Influx")){
				//cb.influxSender = new InfluxSender("127.0.0.1", "8086", "admin", "admin");
				
				if (System.getProperty("influx.server") != null || System.getProperty("influx.port") != null || 
						System.getProperty("influx.user") != null || System.getProperty("influx.pwd") != null){
					cb.influxSender = new InfluxSender(System.getProperty("influx.server"), System.getProperty("influx.port"), System.getProperty("influx.user"), System.getProperty("influx.pwd"));
					cb.influxSender.setup();
				}
				else{
					log.error("Please provide the Influx Server, Port, username & Password");
					System.exit(0);
				}
			}
			else{
				log.error("DB Name Uknown:" + System.getProperty("db.name"));
				System.exit(0);
			}

			//cb.getBucketMetrics("com.amdocs.digital.ms.account.billingaccount");
			cb.getClusterMetrics();
			cb.getBucketMetrics();
		} catch (Exception e) {
			log.error("Exception ", e);
			e.printStackTrace();
			System.exit(0);
		}
	}

	public void createData() {

		DefaultCouchbaseEnvironment.builder().connectTimeout(900000);

		// Initialize Connection
		Cluster cluster = CouchbaseCluster.create("localhost");
		cluster.authenticate("admin", "Admin123");
		Bucket bucket = cluster.openBucket("mybucket");

		// Create JSON Document
		JsonObject km = JsonObject.create().put("name", "Madan")
				.put("email", "kmadan@hp.com");

		JsonArray socialMedia = JsonArray.create();
		socialMedia.add(JsonObject.create().put("title", "Twitter")
				.put("link", "twitter.com/kmadan"));
		socialMedia.add(JsonObject.create().put("title", "Github")
				.put("link", "github.com/kmadan"));

		km.put("socialMedia", socialMedia);

		// Store the Document
		bucket.upsert(JsonDocument.create("kmadan", km));

		// Load the Document and print it
		// Prints Content and Metadata of the stored Document
		System.out.println(bucket.get("kmadan").content());

		// Create a N1QL Primary Index (but ingore if it exists)
		// bucket.bucketManager().createN1qlPrimaryIndex(true, false);

		// Perform a N1QL Query
		// N1qlQueryResult result =
		// bucket.query("SELECT name FROM 'bucketname' WHERE $1 IN interests",
		// JsonArray.from("African Swallows"));
	}

}
