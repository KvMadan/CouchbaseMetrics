/**
 * 
 */
package in.km.oneview.cb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.InfluxDBIOException;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;

/**
 * @author Madan Kavarthapu
 *
 */
public class InfluxSender {
	
	final static Logger log = Logger.getLogger(InfluxSender.class);
	
	private String hostName;
	private String port;
	private String username;
	private String password;
	
	private InfluxDB db;
	
	public InfluxSender(String influxHost, String port, String username, String password){
    	this.hostName = influxHost;
    	this.port = port;
    	this.username = username;
    	this.password = password;
	}
	
	public InfluxDB connectToDB(){
		return InfluxDBFactory.connect("http://127.0.0.1:8086", "admin", "admin");
	}

    private boolean pingServer(InfluxDB influxDB) {
        try {
            // Ping and check for version string
            Pong response = influxDB.ping();
            if (response.getVersion().equalsIgnoreCase("unknown")) {
                log.error("Error pinging server.");
                return false;
            } else {
                log.debug("Database version: " + response.getVersion());
                return true;
            }
        } catch (InfluxDBIOException idbo) {
            log.error("Exception while pinging database: ", idbo);
            return false;
        } catch(Exception e){
        	log.error("Exception: ", e);
        	e.printStackTrace();
        	return false;
        }
    }	
    
    public void setup(){
    	try {
			db = InfluxDBFactory.connect("http://" + hostName + ":" + port, username, password);
			db.setLogLevel(InfluxDB.LogLevel.BASIC);
			pingServer(db);
			db.query(new Query("CREATE DATABASE Couchbase", "Couchbase"));
			db.setConsistency(org.influxdb.InfluxDB.ConsistencyLevel.ALL);
			db.setLogLevel(org.influxdb.InfluxDB.LogLevel.NONE);
			db.setDatabase("Couchbase");
		} catch (Exception e) {
			log.error("Exception:", e);
			e.printStackTrace();
			System.exit(0);
		}
    }
    
    private void writeToDb(InfluxDB influxDB){
    	
    	BatchPoints batchPoints = BatchPoints
    			  .database("Couchbase")
    			  .retentionPolicy("autogen")
    			  .build();
    	
    	Point point1 = Point.measurement("memory")
    			  .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
    			  .addField("name", "server1")
    			  .addField("free", 4743656L)
    			  .addField("used", 1015096L)
    			  .addField("buffer", 1010467L)
    			  .build();
    	
    	Point point2 = Point.measurement("memory")
    			  .time(System.currentTimeMillis() - 100, TimeUnit.MILLISECONDS)
    			  .addField("name", "server1")
    			  .addField("free", 4743696L)
    			  .addField("used", 1016096L)
    			  .addField("buffer", 1008467L)
    			  .build();
    	
    	batchPoints.point(point1);
    	batchPoints.point(point2);
    	influxDB.write(batchPoints);
    }
    
    public void writeMetricsToDB(String table, HashMap<String, Object> map, java.util.Date currentTime){
    	try{
    		
    		Point.Builder builder = Point.measurement(table);
    		
    		Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, Object> pair = it.next();
				
				Double d;
				String s;
				Long i;
				Float f;
				 if (pair.getValue() instanceof Double) {
					 d = (Double)pair.getValue();
					 builder.addField(pair.getKey(), d);
				 }
				 else if (pair.getValue() instanceof Long){
					i = (Long)pair.getValue();
					builder.addField(pair.getKey(), i);
				 }
				 else if (pair.getValue() instanceof Float){
						f = (Float)pair.getValue();
						builder.addField(pair.getKey(), f);
					 }
				 else if (pair.getValue() instanceof String){
					 s = (String)pair.getValue();
					 builder.addField(pair.getKey(), s);
				 }
				it.remove();
			}
			builder.time(currentTime.getTime(), TimeUnit.MILLISECONDS);
			Point point = builder.build();
			log.debug(point.toString());
    		db.write(point);
    		log.debug("Writtern Metrics to DB");
    	}
    	catch(Exception e){
    		log.error("writeMetricsToDB Failed", e);
    		e.printStackTrace();
    	}
    }
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		InfluxSender infl = new InfluxSender("127.0.0.1", "8086", "admin", "admin");
		
		InfluxDB db = infl.connectToDB();
		db.setLogLevel(InfluxDB.LogLevel.BASIC);
		
		infl.pingServer(db);
		
		db.query(new Query("CREATE DATABASE Couchbase", "Couchbase"));
		
		infl.writeToDb(db);
	}
}
