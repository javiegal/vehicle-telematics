package master;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Class to control the average speed between segments 52 and 56. It implements the window function interface.
 */
public class AvgSpeedControl implements WindowFunction<PositionEvent,
        Tuple6<Integer, Integer, Integer, Integer, Integer, Double>, Tuple3<Integer, Integer, Integer>, TimeWindow> {
    private final int windowGap;

    public AvgSpeedControl(int gap) {
        this.windowGap = gap;
    }

    @Override
    public void apply(Tuple3<Integer, Integer, Integer> key, TimeWindow timeWindow,
                      Iterable<PositionEvent> iterable,
                      Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> collector) {

        Iterator<PositionEvent> peIt = iterable.iterator();

        // Start represents the first timestamp of the window (to seconds)
        int start = (int) (timeWindow.getStart() / 1000);
        // End represents the last timestamp: end time of the window (to seconds) minus the window gap
        int end = (int) (timeWindow.getEnd() / 1000 - this.windowGap);
        int posStart = VTConstants.MAX_POS, posEnd = VTConstants.MIN_POS;
        Boolean[] covered = new Boolean[VTConstants.END_SEG - (VTConstants.INI_SEG - 1)];
        Arrays.fill(covered, false);

        while (peIt.hasNext()) {
            PositionEvent next = peIt.next();
            int pos = next.getPos();
            posStart = Integer.min(pos, posStart);
            posEnd = Integer.max(pos, posEnd);
            covered[next.getSeg() - VTConstants.INI_SEG] = true;
        }

        if (!Arrays.asList(covered).contains(false)) {
            double avg = VTConstants.MS_TO_MPH * (posEnd - posStart) / (end - start);

            if (avg > VTConstants.MAX_AVG_SPEED) {
                collector.collect(new Tuple6<Integer, Integer, Integer, Integer, Integer, Double>(start, end,
                        key.getField(0), key.getField(1), key.getField(2), avg));
            }
        }
    }
}
