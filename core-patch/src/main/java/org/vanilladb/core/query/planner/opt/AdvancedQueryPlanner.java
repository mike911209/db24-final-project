// YAHUI: This is the class that we need to modify to implement the advanced query planner.
package org.vanilladb.core.query.planner.opt;

import java.util.ArrayList;
import java.util.List;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.ProjectPlan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.materialize.GroupByPlan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.query.planner.QueryPlanner;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.CoreProperties;
import org.vanilladb.core.query.algebra.AdvancedQueryPlan;
import org.vanilladb.core.query.algebra.LimitPlan;
import org.vanilladb.core.storage.index.ivf.IVFIndex;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.index.SearchRange;

// import org.vanilladb.core.sql.distfn.EuclideanFn;


// ASFINAL: all implemented by table
public class AdvancedQueryPlanner implements QueryPlanner {

    @Override
	public Plan createPlan(QueryData data, Transaction tx) {

        // total K clusters
        List<Float> dist = new ArrayList<Float>(IVFIndex.K);

        // chosen N clusters
        List<Integer> id_centroid = new ArrayList<Integer>(IVFIndex.N);
        List<Plan> trunkPlans = new ArrayList<Plan>(IVFIndex.N);

        Plan trunk = null;
        Plan topPlan = null;

        // Step 1: Calculate the distance between the query and the centroid
        // ASFINAL: global centroid

        IVFIndex idx = VanillaDb.catalogMgr().getIndexInfoByName(IVFIndex.INDEXNAME, tx).openIVF(tx);
        idx.beforeFirst(new SearchRange(new SearchKey()));
        while (idx.next()) {
            float[] vec = (float []) idx.getval(IVFIndex.SCHEMA_VECTOR).asJavaVal();
            float tmp_dist = 0;
            for(DistanceFn queryVec : data.embeddingFields()) {
                tmp_dist += (float)queryVec.distance(new VectorConstant(vec));
            }
            dist.add(tmp_dist);
        }

        // Step 2: Choose the top N smallest distance (map to the centroid id)
        // ASFINAL: optimization
        for (int i = 0; i < IVFIndex.N; i++) {
            float min = Float.MAX_VALUE;
            int min_id = -1;
            if (dist.size() != IVFIndex.K) {
                System.err.println("Error: the number of centroid is not equal to K");
                break;
            }
            for (int j = 0; j < IVFIndex.K; j++) {
                if (dist.get(j) < min) {
                    min = dist.get(j);
                    min_id = j;
                }
            }
            if (min_id == -1) {
                System.err.println("Error: cannot find the centroid id");
                break;
            }
            id_centroid.add(min_id);
            dist.set(min_id, Float.MAX_VALUE);
        }

        
        // Step 3: Create the plans

        // Step 3.1: TablePlan
        if (id_centroid.size() != IVFIndex.N) {
            System.err.println("Error: the number of centroid is not equal to N");
        }
        for (int cluster : id_centroid) {
            trunkPlans.add(new TablePlan(IVFIndex.INDEXNAME + cluster, tx));
        }

        // Step 3.2: GroupByPlan => 照理來講不會執行
        if (data.groupFields() != null) {
            for (int i = 0; i < IVFIndex.N; i++) {
                trunk = new GroupByPlan(trunkPlans.get(i), data.groupFields(), 
                            data.aggregationFn(), tx);
                trunkPlans.set(i, trunk);
            }
        }

        // Step 3.3: SortPlan
        if (data.sortFields() != null) {
            for (int i = 0; i < IVFIndex.N; i++) {
                trunk = new SortPlan(trunkPlans.get(i), data.sortFields(),
                            data.sortDirections(), tx);
                trunkPlans.set(i, trunk);
            }
        }

        // Step 3.4: ProjectPlan
        for (int i = 0; i < IVFIndex.N; i++) {
            trunk = new ProjectPlan(trunkPlans.get(i), data.projectFields());
            trunkPlans.set(i, trunk);
        }

        // Step 3.5: LimitPlan
        if (data.limit() != -1) {
            for (int i = 0; i < IVFIndex.N; i++) {
                trunk = new LimitPlan(trunkPlans.get(i), data.limit());
                trunkPlans.set(i, trunk);
            }
        }

        // Step 3.6: AdvancedPlan
		topPlan = new AdvancedQueryPlan(trunkPlans, data.embeddingFields());

        return topPlan;
    }
}
