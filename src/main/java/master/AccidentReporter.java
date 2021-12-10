package master;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class AccidentReporter implements WindowFunction<PositionEvent, Accident,
            Tuple5<Integer, Integer, Integer, Integer, Integer>, GlobalWindow> {

    @Override
    public void apply(Tuple5<Integer, Integer, Integer,Integer,Integer> keyed, GlobalWindow window,
                      Iterable<PositionEvent> iterable, Collector<Accident> out)
            throws Exception {

              PositionEvent lastElement = new PositionEvent();
              Iterator<PositionEvent> events = iterable.iterator();

              int time1 = events.next().f0;
              int count = 1;

              while (events.hasNext()) {
                  count++;
                  lastElement = events.next();
              }

              if (count == 4)
                  out.collect(new Accident(time1, lastElement.getTime(), keyed.f0, keyed.f1, keyed.f2, keyed.f3,
                          keyed.f4));
    }
}
