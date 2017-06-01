package com.firedrum.log4j2;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.nosql.appender.DefaultNoSqlObject;
import org.apache.logging.log4j.nosql.appender.NoSqlConnection;
import org.apache.logging.log4j.nosql.appender.NoSqlObject;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.dto.BatchPoints;

/**
 * The InfluxDB implementation of {@link NoSqlConnection}.
 */
public final class InfluxDbConnection implements NoSqlConnection<Map<String, Object>, DefaultNoSqlObject> {
	private InfluxDB influxDB;
	private String seriesName;
	private String database;
	@SuppressWarnings("unused")
	private boolean useUdp;
	@SuppressWarnings("unused")
	private Integer udpPort;

	public InfluxDbConnection(String databaseName, String serieName, String url, String username, String password,
			String transport, Integer udpPort) {
		this.database = databaseName;
		this.seriesName = serieName;
		if (username != null && password != null) {
			this.influxDB = InfluxDBFactory.connect(url, username, password);
		} else {
			this.influxDB = InfluxDBFactory.connect(url);
		}
		this.influxDB.createDatabase(this.database);
		// Flush every 2000 Points, at least every 100ms
		this.influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);
		this.influxDB.enableGzip();

		this.useUdp = "UDP".equalsIgnoreCase(transport);
		this.udpPort = udpPort;
	}

	@Override
	public DefaultNoSqlObject createObject() {
		return new DefaultNoSqlObject();
	}

	@Override
	public DefaultNoSqlObject[] createList(int length) {
		return new DefaultNoSqlObject[length];
	}

	@Override
	public void insertObject(NoSqlObject<Map<String, Object>> object) {
		BatchPoints batchPoints = BatchPoints.database(database).consistency(ConsistencyLevel.ALL).build();

		InfluxMapToSeriesConverter converter = new InfluxMapToSeriesConverter(this.seriesName, batchPoints);
		BatchPoints bpoints = converter.convertToBatchPoints(object.unwrap());

		// TODO: did not find the udp impl of the influx-java driver. fix this
		// for using udp.
		influxDB.write(bpoints);
	}

	@Override
	public void close() {
		// Do nothing...
	}

	@Override
	public boolean isClosed() {
		return false;
	}
}
