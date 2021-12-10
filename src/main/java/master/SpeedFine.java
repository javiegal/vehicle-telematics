package master;

import org.apache.flink.api.java.tuple.Tuple6;

/**
 * Class to represent a speed fine
 */
public class SpeedFine extends Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> {
    public SpeedFine() {
        super();
    }

    public SpeedFine(int time, int vid, int xway, int seg, int dir, int spd) {
        super(time, vid, xway, seg, dir, spd);
    }
}
