package com.kappaware.jdchtable;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.TableName;

import com.esotericsoftware.yamlbeans.YamlConfig;

public class Description {
	enum State {
		present, absent
	}

	static final String NAME = "name";
	static final String STATE = "state";
	static final String COLUMN_FAMILIES = "columnFamilies";
	static final String PRESPLIT = "presplit";

	static YamlConfig yamlConfig = new YamlConfig();
	static {
		yamlConfig.setPropertyElementType(Description.class, "namespaces", Namespace.class);
		yamlConfig.setPropertyElementType(Namespace.class, "tables", Table.class);
		yamlConfig.setPropertyElementType(Table.class, "columnFamilies", ColumnFamily.class);
		yamlConfig.setPropertyElementType(Presplit.class, "keysAsNumber", Long.class);
		yamlConfig.writeConfig.setWriteRootTags(false);
		yamlConfig.writeConfig.setWriteRootElementTags(false);
	}

	static public YamlConfig getYamlConfig() {
		return yamlConfig;
	}

	public String zookeeper;
	public String znodeParent;
	public List<Namespace> namespaces;
	
	void polish() throws DescriptionException {
		if(namespaces != null) {
			for(Namespace ns : this.namespaces) {
				ns.polish();
			}
		}
	}

	static public class Namespace {
		public String name;
		public State state;
		public List<Table> tables;
		
		void polish() throws DescriptionException {
			if(this.tables != null) {
				for(Table t : this.tables) {
					t.polish(this.name);
				}
			}
		}
	}

	static public class Table {
		public TableName tableName;
		public String name;
		public State state;
		public List<ColumnFamily> columnFamilies;
		public Presplit presplit;
		public Map<String, Object> properties;

		void polish(String namespaceName) throws DescriptionException {
			if (this.name == null) {
				throw new DescriptionException("Invalid description: Every table must have a 'name' attribute");
			}
			this.tableName = TableName.valueOf(namespaceName, this.name);
			if (this.state == null) {
				this.state = State.present;
			}
			if (this.state == State.present) {
				if (this.columnFamilies == null || this.columnFamilies.size() == 0) {
					throw new DescriptionException(String.format("A table must have a 'columnFamilies' list with at least one item. Not the case for '%s'", this.tableName.toString()));
				}
				for (ColumnFamily cf : this.columnFamilies) {
					cf.polish();
				}
			}
			if (this.presplit != null) {
				this.presplit.polish(this.name.toString());
			}
		}

	}

	static public class Presplit {
		public List<Long> keysAsNumber;
		public List<String> keysAsString;
		public Long startKey;
		public Long endKey;
		public Integer numRegion;

		void polish(String tableName) throws DescriptionException {
			if (this.keysAsNumber != null) {
				if (this.keysAsString != null || this.startKey != null || this.endKey != null || this.numRegion != null) {
					throw new DescriptionException(String.format("keysAsNumber property is exclusive (Table %s)", tableName));
				}
			} else if (this.keysAsString != null) {
				if (this.keysAsNumber != null || this.startKey != null || this.endKey != null || this.numRegion != null) {
					throw new DescriptionException(String.format("keysAsString property is exclusive (Table %s)", tableName));
				}
			} else {
				if (this.startKey == null || this.endKey == null || this.numRegion == null) {
					throw new DescriptionException(String.format("startKey, endKey and numRegion must be defined together (Table %s)", tableName));
				}
			}
		}
	}

	/*
	static public class Table.properties {
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
		public Map<String, Object> properties;

		void polish() throws DescriptionException {
			if (this.name == null) {
				throw new DescriptionException("Invalid description: Every column family must have a 'name' attribute");
			}
			if (this.state == null) {
				this.state = State.present;
			}
		}
	}

	/*
	static public class ColumnFamily.prperties {
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
	*/
}
