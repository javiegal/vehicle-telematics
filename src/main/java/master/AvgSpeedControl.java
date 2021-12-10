package master;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Class to control the average speed between segments 52 and 56. It implements the window function interface.
 */
public class AvgSpeedControl implements WindowFunction<PositionEvent, AvgSpeedFine, Tuple3<Integer, Integer,
        Integer>, TimeWindow> {
    private final int windowGap;

    public AvgSpeedControl(int gap) {
        this.windowGap = gap;
    }

    @Override
    public void apply(Tuple3<Integer, Integer, Integer> key, TimeWindow timeWindow,
                      Iterable<PositionEvent> iterable, Collector<AvgSpeedFine> collector) {

        Iterator<PositionEvent> peIt = iterable.iterator();

        int start = (int) (timeWindow.getStart() / 1000), end = (int) (timeWindow.getEnd() / 1000 - this.windowGap);
        int posStart = Integer.MAX_VALUE, posEnd = 0;
        Boolean[] covered = {false, false, false, false, false};

        while (peIt.hasNext()) {
            PositionEvent next = peIt.next();
            int pos = next.getPos();
            posStart = Integer.min(pos, posStart);
            posEnd = Integer.max(pos, posEnd);
            covered[next.getSeg() - 52] = true;
        }

        if (!Arrays.asList(covered).contains(false)) {
            double avg = (3.6 / 1.609344) * (posEnd - posStart) / (end - start);

            if (avg > 60) {
                collector.collect(new AvgSpeedFine(start, end, key.getField(0), key.getField(1),
                        key.getField(2), avg));
            }
        }
    }
}
