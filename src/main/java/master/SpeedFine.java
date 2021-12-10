package master;

import org.apache.flink.api.java.tuple.Tuple6;

public class SpeedFine extends Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> {
    public SpeedFine() {
    }

    public SpeedFine(int time, int vid, int xway, int seg, int dir, int spd){
        this.f0 = time;
        this.f1 = vid;
        this.f2 = xway;
        this.f3 = seg;
        this.f4 = dir;
        this.f5 = spd;
    }
}
