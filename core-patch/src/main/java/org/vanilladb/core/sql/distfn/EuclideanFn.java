package org.vanilladb.core.sql.distfn;

import org.vanilladb.core.sql.VectorConstant;
import jdk.incubator.vector.*;

public class EuclideanFn extends DistanceFn {

    final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_PREFERRED;

    public EuclideanFn(String fld) {
        super(fld);
    }

    @Override
    protected double calculateDistance(VectorConstant vec) {
        // System.out.println("rrrrrrrrrrrrrr\nrrrr\nr\nrrrrrrrrrrrrrrrrr");
        float[] vec_arr = new float[vec.dimension()];
        float[] query_arr = new float[query.dimension()];
        for (int i=0; i<vec.dimension(); i++) vec_arr[i] = vec.get(i);
        for (int i=0; i<query.dimension(); i++) query_arr[i] = query.get(i);
        
        FloatVector tmp = FloatVector.zero(SPECIES);

        for (int i=0; i<SPECIES.loopBound(vec.dimension()); i+=SPECIES.length()) {
            FloatVector vec_dv = FloatVector.fromArray(SPECIES, vec_arr, i);
            FloatVector query_dv = FloatVector.fromArray(SPECIES, query_arr, i);
            FloatVector diff = vec_dv.sub(query_dv);
 	        tmp = diff.fma(diff, tmp);
        }

        double sum = tmp.reduceLanes(VectorOperators.ADD);
        return Math.sqrt(sum);
    }
    
}
