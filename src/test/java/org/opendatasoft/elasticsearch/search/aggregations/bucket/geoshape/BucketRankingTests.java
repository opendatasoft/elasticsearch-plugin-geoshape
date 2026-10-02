package org.opendatasoft.elasticsearch.search.aggregations.bucket.geoshape;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Covers the order in which buckets compete for the {@code size} and {@code shard_size} slots.
 */
public class BucketRankingTests extends ESTestCase {

    private static InternalGeoShape.InternalBucket point(long shapeHash) {
        return new InternalGeoShape.InternalBucket(new BytesRef(), null, shapeHash, "Point", 0, 1, InternalAggregations.EMPTY, null);
    }

    /** Points all tie on a perimeter of 0, and often on a single document: which ones are kept must not vary. */
    public void testTiesAreBrokenOnTheShapeHash() {
        List<InternalGeoShape.InternalBucket> points = new ArrayList<>();
        for (long shapeHash = 0; shapeHash < 20; shapeHash++) {
            points.add(point(shapeHash));
        }
        Collections.shuffle(points, random());

        InternalGeoShape.BucketPriorityQueue queue = new InternalGeoShape.BucketPriorityQueue(5);
        for (InternalGeoShape.InternalBucket point : points) {
            queue.insertWithOverflow(point);
        }
        Set<Long> kept = new HashSet<>();
        while (queue.size() > 0) {
            kept.add(queue.pop().shapeHash);
        }

        assertEquals(Set.of(15L, 16L, 17L, 18L, 19L), kept);
    }
}
