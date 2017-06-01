package com.firedrum.log4j2;

import org.apache.logging.log4j.Logger;

import org.apache.logging.log4j.LogManager;

public class InfluxDbLog4jAppenderTest {
	private static final Logger logger = LogManager.getLogger(InfluxDbLog4jAppenderTest.class);

	public static void main(final String... args) {
		logger.trace("Entering Test.");
		InfluxDbLog4jAppenderTest test = new InfluxDbLog4jAppenderTest();
		test.doGood();
		test.doBad();
		test.doJSONObject();
		test.doJSONArray();
		logger.trace("Exiting Test.");
		System.exit(0);
	}

	public void doGood() {
		logger.info("Did it.");
	}

	public void doBad() {
		logger.error("Didn't do it.");
	}
	
	public void doJSONObject() {
		logger.info("{\"jsonKey\":\"jsonValue\",\"jsonKey2\":[1,\"string\",{\"jsonSubKey\":{\"jsonSubSubKey\":\"subSubValue\"}}]}");
	}
	
	public void doJSONArray() {
		logger.info("[[1,{\"someKey\":2}],\"string\",{\"jsonSubKey\":{\"jsonSubSubKey\":\"subSubValue\"}}]");
	}
}
