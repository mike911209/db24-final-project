// YAHUI: Scan
package org.vanilladb.core.query.algebra;

import java.util.List;
import java.util.ArrayList;

import org.vanilladb.core.sql.VectorConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.storage.index.ivf.IVFIndex;

public class AdvancedQueryScan implements Scan {

    private List<DistanceFn> embFields = new ArrayList<DistanceFn>();
    private List<Scan> subScans = new ArrayList<Scan>();;
    private List<Float> dist;
    private int curCluster = 0;
    private int curReturn = 0;

    // NOTE: 寫死，根據benchmark的設定，也許可以再修改(?)
    private final int LIMITRETURN = 20;

    public AdvancedQueryScan(List<Scan> subScans, List<DistanceFn> embFields) {
        this.embFields.addAll(embFields);
        this.subScans = subScans;
        dist = new ArrayList<Float>();
        curCluster = 0;
        curReturn = 0;
    }

    @Override
    public void beforeFirst() {
        // TODO Auto-generated method stub
        curReturn = 0;
        for (Scan s : subScans) {
            s.beforeFirst();
        }
        
    }

    @Override
    public Constant getVal(String fldName) {
        // TODO Auto-generated method stub
        Constant ansVal = null;
        float minDist = Float.MAX_VALUE;

        for (int i = 0; i < dist.size(); i++) {
            if (dist.get(i) < minDist) {
                minDist = dist.get(i);
                curCluster = i;
                ansVal = subScans.get(i).getVal(fldName);
            }
        }
        
        return ansVal;
    }

    @Override
    public boolean next() {
        // TODO Auto-generated method stub

        if (curReturn >= LIMITRETURN) {
            return false;
        }

        // 初始化dist
        if (curReturn == 0) {
            for (Scan s : subScans) {
                if (!s.next()) {
                    System.err.println("Error: Not enough data to return.");
                    return false;
                }
                float tmp_dist = 0;
                for (DistanceFn queryVec : embFields) {
                    float[] vec = (float[]) s.getVal(IVFIndex.SCHEMA_VECTOR).asJavaVal();
                    tmp_dist += (float) queryVec.distance(new VectorConstant(vec));
                }
                dist.add(tmp_dist);
            }
        }
        // 如果該cluster不足LIMIT個，則設為MAX_VALUE，避免選到
        else if (!subScans.get(curCluster).next()) {
            dist.set(curCluster, Float.MAX_VALUE);
            return true;
        }
        // 把next過的cluster的dist重新計算
        else {
            float tmp_dist = 0;
            for (DistanceFn queryVec : embFields) {
                float[] vec = (float[]) subScans.get(curCluster).getVal(IVFIndex.SCHEMA_VECTOR).asJavaVal();
                tmp_dist += (float) queryVec.distance(new VectorConstant(vec));
            }
            dist.set(curCluster, tmp_dist);
        }
        curReturn++;
        return true;
    }


    @Override
    public void close() {
        // TODO Auto-generated method stub
        for (Scan s : subScans) {
            s.close();
        }
    }

    @Override
    public boolean hasField(String fldName) {
        // TODO Auto-generated method stub
        for (Scan s : subScans) {
            if (!s.hasField(fldName)) {
                return false;
            }
        }
        return true;
    }


    public void setCluster(int cluster) {
        curCluster = cluster;
    }

}
