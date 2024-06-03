/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.storage.index.ivf;

import java.util.ArrayList;
import java.util.List;

import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VECTOR;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.index.SearchRange;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * A static hash implementation of {@link Index}. A fixed number of buckets is
 * allocated, and each bucket is implemented as a file of index records.
 */
public class IVFIndex extends Index {
	
	/**
	 * A field name of the schema of index records.
	 */
	private static final String SCHEMA_ID = "i_id", SCHEMA_VECTOR = "i_emb";
			

	public static long searchCost(SearchKeyType keyType, long totRecs, long matchRecs) {
		// int rpb = Buffer.BUFFER_SIZE / RecordPage.slotSize(schema(keyType));
		// return (totRecs / rpb) / NUM_BUCKETS;
		return 1;
	}
	
	// private static String keyFieldName(int index) {
	// 	return SCHEMA_KEY + index;
	// }

	/**
	 * Returns the schema of the index records.
	 * 
	 * @return the schema of the index records
	 */
	private static Schema schema() {
		Schema sch = new Schema();
		sch.addField(SCHEMA_ID, INTEGER);
		sch.addField(SCHEMA_VECTOR, VECTOR(128));
		return sch;
	}
	
	private SearchKey searchKey;
	private RecordFile rf;
	private boolean isBeforeFirsted;
	private List<SearchKey> centroidList;
	private static List<SearchKey> recordList = new ArrayList<SearchKey>(1000);

	/**
	 * Opens a hash index for the specified index.
	 * 
	 * @param ii
	 *            the information of this index
	 * @param keyType
	 *            the type of the search key
	 * @param tx
	 *            the calling transaction
	 */
	public IVFIndex(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
		super(ii, keyType, tx);
	}

	@Override
	public void preLoadToMemory() {
		// NOTE: not implemented yet, we can think of what case will this function be used and where to use

		// for (int i = 0; i < NUM_BUCKETS; i++) {
		// 	String tblname = ii.indexName() + i + ".tbl";
		// 	long size = fileSize(tblname);
		// 	BlockId blk;
		// 	for (int j = 0; j < size; j++) {
		// 		blk = new BlockId(tblname, j);
		// 		tx.bufferMgr().pin(blk);
		// 	}
		// }
	}

	/**
	 * Positions the index before the first index record having the specified
	 * search key. The method hashes the search key to determine the bucket, and
	 * then opens a {@link RecordFile} on the file corresponding to the bucket.
	 * The record file for the previous bucket (if any) is closed.
	 * 
	 * @see Index#beforeFirst(SearchRange)
	 */
	@Override
	public void beforeFirst(SearchRange searchRange) {
		// FTODO: open the corresponding record file, store it in some variable
		// 		 the record file will be used in next()
	}

	/**
	 * Moves to the next index record having the search key.
	 * 
	 * @see Index#next()
	 */
	@Override
	public boolean next() {
		// FTODO: call the beforeFirst method first
		// 		 iterate the record file opened in beforeFirst to find the next record
		if (!isBeforeFirsted)
			throw new IllegalStateException("You must call beforeFirst() before iterating index '"
					+ ii.indexName() + "'");
		
		// while (rf.next())
		// 	if (getKey().equals(searchKey))
		// 		return true;
		return false;
	}

	/**
	 * Retrieves the data record ID from the current index record.
	 * 
	 * @see Index#getDataRecordId()
	 */
	@Override
	public RecordId getDataRecordId() {
		// NOTE: This is a dummy function since we don't have data record id in IVF index

		// long blkNum = (Long) rf.getVal(SCHEMA_RID_BLOCK).asJavaVal();
		// int id = (Integer) rf.getVal(SCHEMA_RID_ID).asJavaVal();
		// return new RecordId(new BlockId(dataFileName, blkNum), id);
		return new RecordId(null, 0);
	}

	/**
	 * Inserts a new index record into this index.
	 * 
	 * @see Index#insert(SearchKey, RecordId, boolean)
	 */
	@Override
	public void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
		// NOTE: insert data into static list to be used later in clustering

		// insert the data
		recordList.add(key);
	}

	/**
	 * Deletes the specified index record.
	 * 
	 * @see Index#delete(SearchKey, RecordId, boolean)
	 */
	@Override
	public void delete(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
		// NOTE: not implemented yet

		// search the position
		beforeFirst(new SearchRange(key));
		
		// log the logical operation starts
		if (doLogicalLogging)
			tx.recoveryMgr().logLogicalStart();
		
		// delete the specified entry
		while (next())
			if (getDataRecordId().equals(dataRecordId)) {
				rf.delete();
				return;
			}
		
		// log the logical operation ends
		if (doLogicalLogging)
			tx.recoveryMgr().logIndexDeletionEnd(ii.indexName(), key,
					dataRecordId.block().number(), dataRecordId.id());
	}

	/**
	 * Closes the index by closing the current table scan.
	 * 
	 * @see Index#close()
	 */
	@Override
	public void close() {
		// FTODO: close the corresponding record file that was opened in beforeFirst()

		if (rf != null)
			rf.close();
	}

	public void train(Transaction tx) {
		// obtain lists of clustered records
		List<List<SearchKey>> clusters = clustering();
		System.out.println("clusters size: " + clusters.size());

		// create index files
		for (int i = 0; i < clusters.size(); i++) {
			// create a table file for each cluster
			String tblname = ii.indexName() + i + ".tbl";
			TableInfo ti = new TableInfo(tblname, schema());
			RecordFile rf = ti.open(tx, true);

			// initialize the file header if needed
			if (rf.fileSize() == 0)
				RecordFile.formatFileHeader(ti.fileName(), tx);
			rf.beforeFirst();

			List<SearchKey> cluster = clusters.get(i);

			// insert records
			for (SearchKey record : cluster) {
				rf.insert();
				System.out.println(record.get(0));
				System.out.println(record.get(1));
				System.out.println("record length: " + record.length());
				rf.setVal(SCHEMA_ID, record.get(0));
				rf.setVal(SCHEMA_VECTOR, record.get(1));
			}

			// close the index files
			rf.close();
		}
		
	}

	public Constant getval(String fldName) {
		return rf.getVal(fldName);
	}

	private List<List<SearchKey>> clustering() {
		// FTODO: clustering algorithm
		//		 return the List of List cluster records
		// 		 	* outer List -> clusters
		// 			* inner List -> records in a cluster
		int numRecord = recordList.size();
		int clusterSize = numRecord / 5;
		int count = 1;

		List<List<SearchKey>> clusters = new ArrayList<List<SearchKey>>(5);
		List<SearchKey> cluster = new ArrayList<SearchKey>(clusterSize);

		for (int i = 0; i < numRecord; i++) {
			if (i < count * clusterSize) {
				cluster.add(recordList.get(i));
			} else {
				clusters.add(cluster);
				cluster = new ArrayList<SearchKey>(clusterSize);
				count++;
			}
		}

		return clusters;
	}

	// private long fileSize(String fileName) {
	// 	tx.concurrencyMgr().readFile(fileName);
	// 	return VanillaDb.fileMgr().size(fileName);
	// }
	
	// private SearchKey getKey() {
	// 	Constant[] vals = new Constant[keyType.length()];
	// 	for (int i = 0; i < vals.length; i++)
	// 		vals[i] = rf.getVal(keyFieldName(i));
	// 	return new SearchKey(vals);
	// }
}
