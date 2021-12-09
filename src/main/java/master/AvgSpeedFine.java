package master;

import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.StringUtils;

public class AvgSpeedFine extends Tuple6<Integer, Integer, Integer, Integer, Integer, Double> {
    public AvgSpeedFine(){

    }

    public AvgSpeedFine(int start, int end, int vid, int xway, int dir, double avg) {
        this.f0 = start;
        this.f1 = end;
        this.f2 = vid;
        this.f3 = xway;
        this.f4 = dir;
        this.f5 = avg;
    }

    public String toString() {
        return StringUtils.arrayAwareToString(this.f0) + "," + StringUtils.arrayAwareToString(this.f1) +
                "," + StringUtils.arrayAwareToString(this.f2) + "," + StringUtils.arrayAwareToString(this.f3) +
                "," + StringUtils.arrayAwareToString(this.f4) + "," + StringUtils.arrayAwareToString(this.f5);
    }
}