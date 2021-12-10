package master;

import org.apache.flink.api.java.tuple.Tuple6;

public class SpeedFine extends Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> {

    public SpeedFine() {
    }

    public void setTime(int time) {
        f0 = time;
    }

    public void setVid(int vid) {
        f1 = vid;
    }

    public void setXway(int xway) {
        f2 = xway;
    }

    public void setSeg(int seg) {
        f3 = seg;
    }

    public void setDir(int dir) {
        f4 = dir;
    }

    public void setSpd(int spd) {
        f5 = spd;
    }

}
