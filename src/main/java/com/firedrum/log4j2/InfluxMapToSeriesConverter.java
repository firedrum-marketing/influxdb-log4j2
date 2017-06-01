package com.firedrum.log4j2;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class InfluxMapToSeriesConverter {
	private BatchPoints batchPoints;
	private String measurement;

	private Map<String, Object> pointFields;
	private Map<String, String> tagFields;
	private TimeUnit pointTimeUnit = TimeUnit.MILLISECONDS;
	private Long pointTime;

	public InfluxMapToSeriesConverter(String measure, BatchPoints bp) {
		batchPoints = bp;
		measurement = measure;
		pointFields = new HashMap<String, Object>();
		tagFields = new HashMap<String, String>();
		pointTime = System.currentTimeMillis();
	}

	public BatchPoints convertToBatchPoints(Map<String, Object> map) {
		convertToFields(null, map);
		addFieldToPoint("rawMessage", map.get("message"));
		addPointToBatch();
		return batchPoints;
	}

	private void convertToFields(String parent, Map<String, Object> map) {
		for (Entry<String, Object> entry : map.entrySet()) {
			Object value = entry.getValue();
			if (value == null || value == "") {
				continue;
			}

			String key = entry.getKey();
			if (parent != null) {
				key = parent + "." + key;
			}

			maybeRecurse(key, value);
		}
	}

	private void convertToFields(String key, Collection<?> array) {
		int i = 0;
		Iterator<?> iterator = array.iterator();
		while (iterator.hasNext()) {
			maybeRecurse(key + '[' + i + ']', iterator.next());
			i++;
		}
	}

	@SuppressWarnings("unchecked")
	private void maybeRecurse(String key, Object value) {
		if (value instanceof Map<?, ?>) {
			convertToFields(key, (Map<String, Object>) value);
		} else if (value instanceof Collection<?>) {
			convertToFields(key, (Collection<?>) value);
		} else if (value instanceof Number) {
			addDataToPoint(key, value);
		} else if (value instanceof String) {
			try {
				// Attempt JSON Object parsing
				convertToFields(key, new JSONObject((String) value).toMap());
			} catch (JSONException jsone) {
				try {
					// Attempt JSON Array parsing
					convertToFields(key, new JSONArray((String) value).toList());
					return;
				} catch (JSONException jsone2) {
					addDataToPoint(key, value);
				}
			}
		} else {
			addDataToPoint(key, value.toString());
		}
	}

	private void addDataToPoint(String k, Object v) {
		addFieldToPoint(k, v);
		if (v != null) {
			addTagToPoint(k, v.toString());
		}
	}
	
	private void addFieldToPoint(String k, Object v) {
		pointFields.put(k, v);
	}
	
	private void addTagToPoint(String k, String v) {
		tagFields.put("tag." + k, v);
	}

	private void addPointToBatch() {
		batchPoints.point(Point.measurement(measurement).time(pointTime, pointTimeUnit).fields(pointFields).tag(tagFields).build());
	}
}
