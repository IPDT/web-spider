package com.util;

import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoDB {
	private String host;
	private int port;
	private String dbName;
	private MongoClient mongoClient = null;
	private MongoDatabase mongoDatabase = null;
	
	
	
	public MongoDB(String host, int port, String dbName) {
		this.host = host;
		this.port = port;
		this.dbName = dbName;
	}
	
	public void createCollection(String collectionName) {
		mongoDatabase.createCollection(collectionName);
	}
	public void insertMany(List<Document> documents, String collectionName) {
		MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
		collection.insertMany(documents);
	}
	
	public void insertOne(Document document, String collectionName) {
		MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
		collection.insertOne(document);
	}
	public void con() {
		mongoClient = new MongoClient( host , port );
	    mongoDatabase = mongoClient.getDatabase(dbName);
	}
	public void close() {
		if (mongoClient != null) {
			mongoClient.close();
		}
		
	}
	
}
