package master;

import org.apache.flink.api.java.tuple.Tuple8;

public class PositionEvent extends Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> {
    public PositionEvent(){

    }

    public PositionEvent(int time, int vid, int spd, int xway, int lane, int dir, int seg, int pos) {
        this.f0 = time;
        this.f1 = vid;
        this.f2 = spd;
        this.f3 = xway;
        this.f4 = lane;
        this.f5 = dir;
        this.f6 = seg;
        this.f7 = pos;
    }

    public Integer getTime() {
        return this.f0;
    }

    public Integer getVid() {
        return this.f1;
    }

    public Integer getSpd() {
        return this.f2;
    }

    public Integer getXway() {
        return this.f3;
    }

    public Integer getLane() {
        return this.f4;
    }

    public Integer getDir() {
        return this.f5;
    }

    public Integer getSeg() {
        return this.f6;
    }

    public Integer getPos() {
        return this.f7;
    }
}
