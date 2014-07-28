package org.elasticsearch.datagen;

public class DataGenerator {
	public static void main(String[] args) {
		String hostName = getProperty("es.host", "localhost");
		int port = getProperty("es.port", 9200);
		
		new DataGenerator().run(hostName, port);
	}
	
	private void run(String hostName, int port) {
		// TODO Auto-generated method stub
		
	}

	public static String getProperty(String key, String defaultValue) {
		return System.getProperty(key, defaultValue);
	}
	
	public static int getProperty(String key, int defaultValue) {
		int intValue = defaultValue;
		try {
			String value = System.getProperty(key);
			if (value != null) {
				intValue = Integer.parseInt(value);
			}
		} catch (NumberFormatException ignored) {
		}
		
		return intValue;
	}
}
