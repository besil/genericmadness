import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by besil on 9/25/15.
 */
public class SimpleTest {
    @Test
    public void test() {
        JavaSparkContext jsc = new JavaSparkContext("local[4]", "testapp");

        final JavaRDD<String> words = jsc.parallelize(Arrays.asList("hello", "world"));
//        final JavaPairRDD<String, String> wordCount = words.mapToPair(word -> new Tuple2(word, "1"));

        final JavaPairRDD<String, Long> rdd = words
                .mapToPair(word -> new Tuple2(word, "1"))
                .countApproxDistinctByKey(0.01);

        rdd.collect().forEach(System.out::println);

    }
}