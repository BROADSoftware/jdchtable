package com.kappaware.jdchtable;

import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.KeepDeletedCells;
//import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;

import com.esotericsoftware.yamlbeans.YamlConfig;


public class HDescription {
	enum State {
		present,
		absent
	}
	

	static YamlConfig yamlConfig = new YamlConfig();
	static {
		yamlConfig.setPropertyElementType(HDescription.class, "namespaces", Namespace.class);
		//yamlConfig.setPropertyElementType(Namespace.class, "tables", Table.class);
		//yamlConfig.setPropertyElementType(Table.class, "columnFamilies", ColumnFamily.class);
		yamlConfig.writeConfig.setWriteRootTags(false);
		yamlConfig.writeConfig.setWriteRootElementTags(false);
	}

	static public YamlConfig getYamlConfig() {
		return yamlConfig;
	}	

	public String zookeeper;
	public String znodeParent;
	public List<Namespace> namespaces;
	
	
	static public class Namespace {
		public String name;
		public State state;
		public List<Table> tables;
	}

	@SuppressWarnings("serial")
	static public class Table extends HashMap<String, Object> {
	}
	
	/*
	static public class Table {
		public String name;
		public State state;
		public List<ColumnFamily> columnFamilies;
		public Boolean compactionEnabled;
		public Durability durability;
		public Long maxFileSize;
		public Long memStoreFlushSize;
		public Boolean readOnly;
		public Boolean regionMemstoreReplication;
		public Integer regionReplication;
	}
	*/

	static public class ColumnFamily {
		public String name;
		public State state;
		public Boolean blockCacheEnabled;
		public Integer blockSize;
		public BloomType bloomFilterType;
		public Boolean cacheBloomsOnWrite;
		public Boolean cacheDataInL1;
		public Boolean cacheDataOnWrite;
		public Boolean cacheIndexesOnWrite;
		public Compression.Algorithm compactionCompressionType;
		public Compression.Algorithm compressionType;
		public Boolean compressTags;
		public DataBlockEncoding dataBlockEncoding;
		public Boolean evictBlockOnClose;
		public Boolean inMemory;
		public KeepDeletedCells keepDeletedCells;
		public Integer maxVersions;
		public Integer minVersions;
		public Boolean prefetchBlockOnOpen;
		public Integer scope;
		public Integer timeToLive;
		
	}
 }
