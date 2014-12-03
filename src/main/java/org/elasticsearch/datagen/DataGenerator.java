package org.elasticsearch.datagen;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import com.fasterxml.jackson.databind.ObjectMapper;

public class DataGenerator {
	private static Random rand = new Random();
	private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
	private static ObjectMapper mapper = new ObjectMapper();
	
	private static final String[] CATEGORIES = {
		"Home", "Grocery", "Outdoor", "Hardware", "Plumbing", "Clothing"
	};
	
	private static final float[] PRICES = {
		23.99f, 99.99f, 50f, 100f, 299.95f, 88.50f, 37.25f, 5.77f
	};
	
	private static final String[] DESCRIPTIONS = {
		"Bottle Opener",
		"Toilet Paper (single-ply)",
		"Toilet Paper (2-ply)",
		"Paper Plates (500x)",
		"Paper Plates (150x)",
		"PVC Pipe - L-joint - 3.25\"",
		"Paint - White",
		"Lawn Chair",
		"Screwdriver",
		"Polo Shirt",
		"Pepperoni Pizza"
	};
	
	private static final int[] UNITS = {
		1, 2, 3, 5, 17, 25, 99
	};
	
	public static void main(String[] args) throws Exception {
		new DataGenerator().run();
	}
	
	private static Client getClient() {
		String clusterName = getProperty("es.cluster.name", "elasticsearch");
		Settings settings = ImmutableSettings
				.settingsBuilder()
				.put("cluster.name", clusterName)
				.build();
		
		String[] hosts = getProperty("es.unicast.hosts", "localhost").split(",");
		int port = getProperty("es.unicast.port", 9300);
		
		TransportClient client = new TransportClient(settings);
		for (String host : hosts) {
			client.addTransportAddress(new InetSocketTransportAddress(host, port));
		}
		
		return client;
	}
	
	private static BulkRequestBuilder getRequest(Client client) {
		return client.prepareBulk()
				.setConsistencyLevel(WriteConsistencyLevel.DEFAULT)
				.setReplicationType(ReplicationType.SYNC)
				.setRefresh(false)
				.setTimeout("30s");
	}
	
	private void run() throws Exception {
		String index = getProperty("es.index.name", "items");
		String mapping = getProperty("es.mapping.name", "purchases");
		Client client = getClient();
		BulkRequestBuilder request = getRequest(client);
		
		int durationMinutes = getProperty("es.duration", 30);
		int volPerSec = getProperty("es.volume", 50);
		
		long now = System.currentTimeMillis();
		long then = now - (durationMinutes * 60 * 1000l);
		for (long x = then; x <= now; x += 1000) {
			for (int y = 0, r = rand.nextInt(volPerSec) + 1; y < r; y++) {
				String descr = rand(DESCRIPTIONS);
				String cat = rand(CATEGORIES);
				int units = rand(UNITS);
				float price = rand(PRICES);
				float total = price * units;
				Date date = new Date(x);
				
//				System.out.printf("%-35s %-40s %-12s $%7.2f %5d    $%9.2f", date.toString(), descr, cat, price, units, total);
//				System.out.println();
				
				Map<String, Object> map = new LinkedHashMap<String, Object>();
				map.put("timestamp", df.format(date));
				map.put("description", descr);
				map.put("category", cat);
				map.put("units", units);
				map.put("price", price);
				map.put("total", total);
				
				String json = mapper.writeValueAsString(map);
//				System.out.println(json);
				
				request.add(client.prepareIndex(index, mapping).setSource(json));
			}
			
			request.execute().actionGet();
			request = getRequest(client);
		}
		
		client.close();
	}
	
	private static String rand(String[] elems) {
		int index = rand.nextInt(elems.length);
		return elems[index];
	}
	
	private static float rand(float[] elems) {
		int index = rand.nextInt(elems.length);
		return elems[index];
	}
	
	private static int rand(int[] elems) {
		int index = rand.nextInt(elems.length);
		return elems[index];
	}

	private static String getProperty(String key, String defaultValue) {
		return System.getProperty(key, defaultValue);
	}
	
	private static int getProperty(String key, int defaultValue) {
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
