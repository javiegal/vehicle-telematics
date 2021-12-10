package master;

import org.apache.flink.api.java.tuple.Tuple8;

public class PositionEvent extends Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> {
    public PositionEvent() {
        super();
    }

    public PositionEvent(int time, int vid, int spd, int xway, int lane, int dir, int seg, int pos) {
        super(time, vid, spd, xway, lane, dir, seg, pos);
    }

    public Integer getTime() {
        return super.getField(0);
    }

    public Integer getVid() {
        return super.getField(1);
    }

    public Integer getSpd() {
        return super.getField(2);
    }

    public Integer getXway() {
        return super.getField(3);
    }

    public Integer getLane() {
        return super.getField(4);
    }

    public Integer getDir() {
        return super.getField(5);
    }

    public Integer getSeg() {
        return super.getField(6);
    }

    public Integer getPos() {
        return super.getField(7);
    }
}
