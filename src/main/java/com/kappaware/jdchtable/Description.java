package com.kappaware.jdchtable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.hbase.TableName;

import com.esotericsoftware.yamlbeans.YamlConfig;

public class Description {
	enum State {
		present, absent
	}

	static final String NAME = "name";
	static final String STATE = "state";
	static final String COLUMN_FAMILIES = "columnFamilies";

	static YamlConfig yamlConfig = new YamlConfig();
	static {
		yamlConfig.setPropertyElementType(Description.class, "namespaces", Namespace.class);
		yamlConfig.setPropertyElementType(Namespace.class, "tables", Table.class);
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
		private TableName name;
		private State state;
		private List<ColumnFamily> columnFamilies = new Vector<ColumnFamily>();

		/**
		 * YamlParser set the value in the base HashMap instead of explicit property. So, we must transfer it to specific property. This will allow:
		 * - Easiest access
		 * - Only properties remaining in the base HashMap will be applied to target object (HTableDescriptor in this case)
		 * @param namespaceName
		 * @throws DescriptionException
		 */
		@SuppressWarnings("unchecked")
		void polish(String namespaceName) throws DescriptionException {
			if (this.get(NAME) == null) {
				throw new DescriptionException("Invalid description: Every table must have a 'name' attribute");
			}
			this.name = TableName.valueOf(namespaceName, this.get(NAME).toString());
			this.remove(NAME);
			this.state = Utils.parseEnum(State.class, this.get(STATE), "table.state", State.present);
			this.remove(STATE);

			if (this.state == State.present) {
				if (this.get(COLUMN_FAMILIES) == null || !(this.get(COLUMN_FAMILIES) instanceof List<?>) || ((List<?>) this.get(COLUMN_FAMILIES)).size() == 0) {
					throw new DescriptionException(String.format("A table must have a 'columnFamilies' list with at least one item. Not the case for %s", this.name.toString()));
				}
				for (Object cf : ((List<?>) this.get(COLUMN_FAMILIES))) {
					if (!(cf instanceof Map<?, ?>)) {
						throw new DescriptionException(String.format("Table %s: columnFamilies list must contains one or several map. (as: - name: xxxxx)", this.name.toString()));
					}
					ColumnFamily columnFamily = new ColumnFamily((Map<String,Object>)cf);
					columnFamily.polish();
					this.columnFamilies.add(columnFamily);
				}
			}
			this.remove(COLUMN_FAMILIES);
		}

		public TableName getName() {
			return name;
		}

		public State getState() {
			return state;
		}

		public List<ColumnFamily> getColumnFamilies() {
			return columnFamilies;
		}

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

	@SuppressWarnings("serial")
	static public class ColumnFamily extends HashMap<String, Object> {
		private String name;
		private State state;

		ColumnFamily(Map<String, Object> m) {
			super(m);
		}

		void polish() throws DescriptionException {
			if (this.get(NAME) == null) {
				throw new DescriptionException("Invalid description: Every column family must have a 'name' attribute");
			}
			this.name = this.get(NAME).toString();
			this.remove(NAME);
			this.state = Utils.parseEnum(State.class, this.get(STATE), "table.state", State.present);
			this.remove(STATE);
		}

		public String getName() {
			return name;
		}

		public State getState() {
			return state;
		}
	}

	/*
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
	*/
}
