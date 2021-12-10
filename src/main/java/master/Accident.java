package master;

import org.apache.flink.api.java.tuple.Tuple7;

public class Accident extends Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> {
    public Accident(){

    }

    public Accident(int startTime, int endTime, int vid, int xway, int seg, int dir, int pos) {
        this.f0 = startTime;
        this.f1 = endTime;
        this.f2 = vid;
        this.f3 = xway;
        this.f4 = seg;
        this.f5 = dir;
        this.f6 = pos;
    }

}
