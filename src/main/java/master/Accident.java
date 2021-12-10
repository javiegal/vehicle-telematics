package master;

import org.apache.flink.api.java.tuple.Tuple7;

/**
 * Class to represent an accident event
 */
public class Accident extends Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> {
    public Accident() {
        super();
    }

    public Accident(int startTime, int endTime, int vid, int xway, int seg, int dir, int pos) {
        super(startTime, endTime, vid, xway, seg, dir, pos);
    }

}
