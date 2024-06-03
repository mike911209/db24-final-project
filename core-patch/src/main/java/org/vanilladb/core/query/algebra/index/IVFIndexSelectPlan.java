/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
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
package org.vanilladb.core.query.algebra.index;

import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VECTOR;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.index.ivf.IVFIndex;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The {@link Plan} class corresponding to the <em>indexselect</em> relational
 * algebra operator.
 */
public class IVFIndexSelectPlan implements Plan {
	private IndexInfo ii;
	private Transaction tx;
	private Histogram hist;

    private static final String SCHEMA_ID = "i_id", SCHEMA_VECTOR = "i_emb",
    	IDX_NAME = "idx_sift";

	/**
	 * Creates a new index-select node in the query tree for the specified index
	 * and search range.
	 * @param ii
	 *            information about the index
	 * @param tx
	 *            the calling transaction
	 */
	public IVFIndexSelectPlan(Transaction tx) {
		this.ii = VanillaDb.catalogMgr().getIndexInfoByName(IDX_NAME, tx);
		this.tx = tx;
		// hist = SelectPlan.constantRangeHistogram(tp.histogram(), searchRanges);
	}

	/**
	 * Creates a new index-select scan for this query
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		IVFIndex idx = ii.openIVF(tx);
		return new IVFIndexSelectScan(idx);
	}

	/**
	 * Estimates the number of block accesses to compute the index selection,
	 * which is the same as the index traversal cost plus the number of matching
	 * data records.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		// return IVFIndex.searchCost(ii.indexType(), new SearchKeyType(schema(), ii.fieldNames()),
		// 		tp.recordsOutput(), recordsOutput()) + recordsOutput();
        return 1;
	}

	/**
	 * Returns the schema of the data table.
	 * 
	 * @see Plan#schema()
	 */
	@Override
	public Schema schema() {
        Schema sch = new Schema();
		sch.addField(SCHEMA_ID, INTEGER);
		sch.addField(SCHEMA_VECTOR, VECTOR(128));
		return sch;
	}

	/**
	 * Returns the histogram that approximates the join distribution of the
	 * field values of query results.
	 * 
	 * @see Plan#histogram()
	 */
	@Override
	public Histogram histogram() {
		return hist;
	}

	@Override
	public long recordsOutput() {
		// return (long) histogram().recordsOutput();
		return 1;
	}

	@Override
	public String toString() {
		// String c = tp.toString();
        String c = "";
		String[] cs = c.split("\n");
		StringBuilder sb = new StringBuilder();
		sb.append("->");
		// sb.append("IndexSelectPlan cond:" + searchRanges.toString() + " (#blks="
		// 		+ blocksAccessed() + ", #recs=" + recordsOutput() + ")\n");
		for (String child : cs)
			sb.append("\t").append(child).append("\n");
		return sb.toString();
	}
}
