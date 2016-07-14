/*
 * Copyright (C) 2016 BROADSoftware
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kappaware.jdchtable;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.TableName;

import com.esotericsoftware.yamlbeans.YamlConfig;

public class Description {
	public enum State {
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
	
	void polish(State defaultState) throws DescriptionException {
		if(namespaces != null) {
			for(Namespace ns : this.namespaces) {
				ns.polish(defaultState);
			}
		}
	}

	static public class Namespace {
		public String name;
		public State state;
		public List<Table> tables;
		
		void polish(State defaultState) throws DescriptionException {
			if(this.state == null) {
				this.state = defaultState;
			}
			if(this.tables != null) {
				for(Table t : this.tables) {
					t.polish(this.name, defaultState);
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

		void polish(String namespaceName, State defaultState) throws DescriptionException {
			if (this.name == null) {
				throw new DescriptionException("Invalid description: Every table must have a 'name' attribute");
			}
			this.tableName = TableName.valueOf(namespaceName, this.name);
			if (this.state == null) {
				this.state = defaultState;
			}
			if (this.state == State.present) {
				if (this.columnFamilies == null || this.columnFamilies.size() == 0) {
					throw new DescriptionException(String.format("A table must have a 'columnFamilies' list with at least one item. Not the case for '%s'", this.tableName.toString()));
				}
				for (ColumnFamily cf : this.columnFamilies) {
					cf.polish(defaultState);
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

		void polish(State defaultState) throws DescriptionException {
			if (this.name == null) {
				throw new DescriptionException("Invalid description: Every column family must have a 'name' attribute");
			}
			if (this.state == null) {
				this.state = defaultState;
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
