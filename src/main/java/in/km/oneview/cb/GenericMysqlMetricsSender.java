/**
 * 
 */
package in.km.oneview.cb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * @author Madan Kavarthapu
 *
 */
@SuppressWarnings("unused")
public class GenericMysqlMetricsSender {

	final static Logger log = Logger.getLogger(GenericMysqlMetricsSender.class);

	private String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private String DB_URL = "jdbc:mysql://%s:%s/";
	private Connection connection = null;
	private Statement statement;
	private String DB_CREATE_SQL = "CREATE DATABASE %s";

	private String TABLE_CREATE_CB_SQL = "CREATE TABLE IF NOT EXISTS %s.cb ("
			+ "  `tStamp` datetime(6) NOT NULL," + "  `clusterName` varchar(50),"
			+ "  `CL_RAM_USED` decimal(12,3),"
			+ "  `CL_RAM_USED_FOR_DATA` decimal(12,3),"
			+ "  `CL_RAM_TOTAL` decimal(12,3),"
			+ "  `CL_HDD_TOTAL` decimal(12,3),"
			+ "  `CL_HDD_USED_FOR_DATA` decimal(12,3),"
			+ "  `CL_HDD_USED` decimal(12,3),"
			+ "  `CL_HDD_FREE` decimal(12,3),"
			+ "  `ftsMemoryQuota` decimal(12,3),"
			+ "  `indexMemoryQuota` decimal(12,3),"
			+ "  `memoryQuota` decimal(12,3),"
			+ "  `maxBucketCount` decimal(12,3),"
			+ "  `rebalance_success` decimal(12,3),"
			+ "  `rebalance_start` decimal(12,3),"
			+ "  `rebalance_stop` decimal(12,3),"
			+ "  `rebalance_fail` decimal(12,3),"
			+ "  `failover_node` decimal(12,3),"
			+ "  `indexFragmentationThreshold` decimal(12,3),"
			+ "  `indexCompactionMode` decimal(12,3),"
			+ "  `viewFragmentationThreshold` decimal(12,3),"
			+ "  `databaseFragmentationThreshold` decimal(12,3),"
			+ "  `parallelDBAndViewCompaction` decimal(12,3)"
			+ "  )";
	
	private String TABLE_CREATE_CB_NODES_SQL = "CREATE TABLE IF NOT EXISTS %s.cbnodes ("
			+ "  `tStamp` datetime(6) NOT NULL,"
			+ "  `ND_NAME` varchar(50),"
			+ "  `ND_VERSION` varchar(50),"
			+ "  `ND_CPU_USED` decimal(12,3),"
			+ "  `ND_SWAP_TOTAL` decimal(12,3),"
			+ "  `ND_SWAP_USED` decimal(12,3),"
			+ "  `ND_MEM_TOTAL` decimal(12,3),"
			+ "  `ND_MEM_FREE` decimal(12,3),"
			+ "  `ND_UPTIME` decimal(12,3),"
			+ "  `couch_docs_actual_disk_size` decimal(12,3),"
			+ "  `couch_docs_data_size` decimal(12,3),"
			+ "  `couch_spatial_data_size` decimal(12,3),"
			+ "  `couch_spatial_disk_size` decimal(12,3),"
			+ "  `couch_views_actual_disk_size` decimal(12,3),"
			+ "  `couch_views_data_size` decimal(12,3),"
			+ "  `curr_items` decimal(12,3),"
			+ "  `curr_items_tot` decimal(12,3),"
			+ "  `ep_bg_fetched` decimal(12,3),"
			+ "  `get_hits` decimal(12,3),"
			+ "  `mem_used` decimal(12,3),"
			+ "  `ops` decimal(12,3),"
			+ "  `vb_replica_curr_items` decimal(12,3),"
			+ "  `memoryTotal` decimal(12,3),"
			+ "  `memoryFree` decimal(12,3),"
			+ "  `mcdMemoryReserved` decimal(12,3),"
			+ "  `mcdMemoryAllocated` decimal(12,3),"
			+ "  `ND_ClusterMembership` varchar(15),"
			+ "  `ND_Status` varchar(15)"
			+ "  )";

	private String TABLE_CREATE_CB_BUCKET_SQL = "CREATE TABLE IF NOT EXISTS %s.cbbuckets ("
			+ "  `tStamp` datetime(6) NOT NULL," + "  `bucketName` varchar(100),"
			+ "  `quotaPercentUsed` decimal(12,3),"
			+ "  `opsPerSec` decimal(12,3),"
			+ "  `diskFetches` decimal(12,3),"
			+ "  `itemCount` decimal(12,3),"
			+ "  `diskUsed` decimal(12,3),"
			+ "  `dataUsed` decimal(12,3),"
			+ "  `memUsed` decimal(12,3),"
			+ "  `ram` decimal(12,3),"
			+ "  `rawRAM` decimal(12,3),"
			+ "  `bucketType` varchar(15)"
			+ "  )";

	private String INSERT_TRANSACTIONS_SQL = "INSERT INTO %s.cb ( %s ) VALUES ( %s )";
	private String INSERT_NODE_SQL = "INSERT INTO %s.cbnodes ( %s ) VALUES ( %s )";
	private String INSERT_BUCKET_SQL = "INSERT INTO %s.cbbuckets ( %s ) VALUES ( %s )";

	private String TABLE_CB_SQL = "SELECT count(*) FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'";
	private String TABLE_CB_NODE_SQL = "SELECT count(*) FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'";
	private String TABLE_CB_BUCKET_SQL = "SELECT count(*) FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'";

	public void setup(String mysqlHost, String mysqlPort, String cbSchema,
			String userName, String password) {
		boolean schemaExists = false;
		try {
			Class.forName(JDBC_DRIVER);

			connection = DriverManager.getConnection(
					String.format(DB_URL, mysqlHost, mysqlPort), userName,
					password);

			ResultSet resultSet = connection.getMetaData().getCatalogs();

			// iterate each schema in the ResultSet
			log.debug("**List of available Schemas**");
			while (resultSet.next()) {
				// Get the database name, which is at position 1
				String databaseName = resultSet.getString(1);
				log.debug(databaseName);
				if (databaseName.equalsIgnoreCase(cbSchema)) {
					log.debug("Database already exists");
					schemaExists = true;
					break;
				}
			}
			resultSet.close();

			statement = connection.createStatement();

			if (!schemaExists) {
				// Create a new Schema with the given name.

				statement.executeUpdate(String.format(DB_CREATE_SQL, cbSchema));
				log.debug("Database created successfully..");
				// Create Required Tables in DB.
				statement.executeUpdate(String.format(TABLE_CREATE_CB_SQL,
						cbSchema));
				//Create Node Details Table in DB.
				statement.executeUpdate(String.format(
						TABLE_CREATE_CB_NODES_SQL, cbSchema));
				//Create Bucket Details Table in DB.
				statement.executeUpdate(String.format(
						TABLE_CREATE_CB_BUCKET_SQL, cbSchema));
				
				log.debug("Table(s) Created Successfully");
			} else {
				TABLE_CB_SQL = String.format(TABLE_CB_SQL, cbSchema, "cb");
				log.debug(TABLE_CB_SQL);
				resultSet = statement.executeQuery(TABLE_CB_SQL);

				if (resultSet.first()) {
					// Get the table count with name eg, which is at position 1
					int tableCount = resultSet.getInt(1);
					log.debug("Is eg table Exists?? - "
							+ (tableCount == 1 ? "Yes" : "No"));
					if (tableCount == 0) {
						// Create Required Tables in DB.
						statement.executeUpdate(String.format(
								TABLE_CREATE_CB_SQL, cbSchema));
						log.debug("Table Created Successfully");
					}
				}
				resultSet.close();
				
				TABLE_CB_NODE_SQL = String.format(TABLE_CB_NODE_SQL,
						cbSchema, "cbNode");
				log.debug(TABLE_CB_NODE_SQL);
				resultSet = statement.executeQuery(TABLE_CB_NODE_SQL);

				if (resultSet.first()) {
					// Get the table count with name cbNode, which is at
					// position 1
					int tableCount = resultSet.getInt(1);
					log.debug("Is egreport table Exists?? - "
							+ (tableCount == 1 ? "Yes" : "No"));
					if (tableCount == 0) {
						// Create Required Tables in DB.
						statement.executeUpdate(String.format(
								TABLE_CREATE_CB_NODES_SQL, cbSchema));
						log.debug("Table Created Successfully");
					}
				}
				resultSet.close();
				
				TABLE_CB_BUCKET_SQL = String.format(TABLE_CB_BUCKET_SQL,
						cbSchema, "cbBucket");
				log.debug(TABLE_CB_BUCKET_SQL);
				resultSet = statement.executeQuery(TABLE_CB_BUCKET_SQL);

				if (resultSet.first()) {
					// Get the table count with name cbNode, which is at
					// position 1
					int tableCount = resultSet.getInt(1);
					log.debug("Is egreport table Exists?? - "
							+ (tableCount == 1 ? "Yes" : "No"));
					if (tableCount == 0) {
						// Create Required Tables in DB.
						statement.executeUpdate(String.format(
								TABLE_CREATE_CB_BUCKET_SQL, cbSchema));
						log.debug("Table Created Successfully");
					}
				}
				resultSet.close();
			}

			// Update Schema name
			INSERT_TRANSACTIONS_SQL = String.format(INSERT_TRANSACTIONS_SQL,
					cbSchema, "%s", "%s");
			INSERT_NODE_SQL = String.format(
					INSERT_NODE_SQL, cbSchema, "%s", "%s");
			INSERT_BUCKET_SQL = String.format(
					INSERT_BUCKET_SQL, cbSchema, "%s", "%s");
			

			connection.setCatalog(cbSchema);
			log.debug("Catalog: " + connection.getCatalog());

			// move this to destroy method.
			// connection.close();

		} catch (ClassNotFoundException e) {
			log.error("ClassNotFound ", e);
			e.printStackTrace();
		} catch (SQLException e) {
			log.error("SQLException ", e);
			e.printStackTrace();
		}
	}

	public void writeClusterMetricsToDB(String testName, 
			HashMap<String, Object> map, String currentTime) {

		try {
			StringBuffer keys = new StringBuffer();
			StringBuffer values = new StringBuffer();
			
			// Adding TestName
			//keys.append("testName" + ",");
			//values.append("'" + testName + "',");

			// Adding Event Type
			//keys.append("eventtype" + ",");
			//values.append("'" + eventType + "',");

			// Adding DatTime
			keys.append("tStamp" + ",");
			values.append("'" + currentTime + "',");

			Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, Object> pair = it.next();
				// System.out.println(pair.getKey() + " --> " +
				// pair.getValue());

				keys.append(pair.getKey() + ",");
				values.append("'" + pair.getValue() + "',");

				it.remove();
			}

			keys.deleteCharAt(keys.length() - 1);
			values.deleteCharAt(values.length() - 1);

			String sql = String.format(INSERT_TRANSACTIONS_SQL,
					keys.toString(), values.toString());
			log.debug(sql);
			statement = connection.createStatement();
			int count = statement.executeUpdate(sql);
			log.debug("Rows Inserted in DB: " + count);
			// log.debug("DB Inserting Disabled");

		} catch (SQLException se) {
			log.error("SQLException", se);
			se.printStackTrace();
		} catch (Exception e) {
			log.error("Exception", e);
			e.printStackTrace();
		}
	}
	
	public void writeNodeMetricsToDB( 
			HashMap<String, Object> map, String currentTime) {
		try {
			StringBuffer keys = new StringBuffer();
			StringBuffer values = new StringBuffer();
			// Adding TestName
			//keys.append("testName" + ",");
			//values.append("'" + testName + "',");

			// Adding DatTime
			keys.append("tStamp" + ",");
			values.append("'" + currentTime + "',");

			Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, Object> pair = it.next();
				// System.out.println(pair.getKey() + " --> " +
				// pair.getValue());

				keys.append(pair.getKey() + ",");
				values.append("'" + pair.getValue() + "',");

				it.remove();
			}

			keys.deleteCharAt(keys.length() - 1);
			values.deleteCharAt(values.length() - 1);

			String sql = String.format(INSERT_NODE_SQL,
					keys.toString(), values.toString());
			log.debug(sql);
			statement = connection.createStatement();
			int count = statement.executeUpdate(sql);
			log.debug("Rows Inserted in DB: " + count);
			// log.debug("DB Inserting Disabled");

		} catch (SQLException se) {
			log.error("SQLException", se);
			se.printStackTrace();
		} catch (Exception e) {
			log.error("Exception", e);
			e.printStackTrace();
		}
	}
	
	public void writeBucketMetricsToDB(String bucketName, 
			HashMap<String, Object> map, String currentTime) {
		try {
			StringBuffer keys = new StringBuffer();
			StringBuffer values = new StringBuffer();
			// Adding TestName
			keys.append("bucketName" + ",");
			values.append("'" + bucketName + "',");

			// Adding DatTime
			keys.append("tStamp" + ",");
			values.append("'" + currentTime + "',");

			Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, Object> pair = it.next();
				// System.out.println(pair.getKey() + " --> " +
				// pair.getValue());

				keys.append(pair.getKey() + ",");
				values.append("'" + pair.getValue() + "',");

				it.remove();
			}

			keys.deleteCharAt(keys.length() - 1);
			values.deleteCharAt(values.length() - 1);

			String sql = String.format(INSERT_BUCKET_SQL,
					keys.toString(), values.toString());
			log.debug(sql);
			statement = connection.createStatement();
			int count = statement.executeUpdate(sql);
			log.debug("Rows Inserted in DB: " + count);
			// log.debug("DB Inserting Disabled");

		} catch (SQLException se) {
			log.error("SQLException", se);
			se.printStackTrace();
		} catch (Exception e) {
			log.error("Exception", e);
			e.printStackTrace();
		}
	}

	public void destroy() {
		// closing the MYSQL Connection
		try {
			connection.close();
		} catch (SQLException e) {
			log.error("SQLException", e);
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
			}
		}
	}
}
