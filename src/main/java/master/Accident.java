package master;

import org.apache.flink.api.java.tuple.Tuple7;

public class Accident extends Tuple7<Integer, Integer, Integer,
        Integer, Integer, Integer, Integer> {

    public Accident() {
    }

    public void setSTime(int time) {
        f0 = time;
    }

    public void setFTime(int time) {
        f1 = time;
    }

    public void setVid(int vid) {
        f2 = vid;
    }

    public void setXway(int xway) {
        f3 = xway;
    }

    public void setSeg(int seg) {
        f4 = seg;
    }

    public void setDir(int dir) {
        f5 = dir;
    }

    public void setPos(int pos) {
        f6 = pos;
    }

}
