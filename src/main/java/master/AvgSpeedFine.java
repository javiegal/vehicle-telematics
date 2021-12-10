package master;

import org.apache.flink.api.java.tuple.Tuple6;

/**
 * Class to represent an average speed fine
 */
public class AvgSpeedFine extends Tuple6<Integer, Integer, Integer, Integer, Integer, Double> {
    public AvgSpeedFine() {
        super();
    }

    public AvgSpeedFine(int start, int end, int vid, int xway, int dir, double avg) {
        super(start, end, vid, xway, dir, avg);
    }
}
