package master;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;

public class RepOfAccident implements WindowFunction<PositionEvent, Accident,
            Tuple5<Integer, Integer, Integer, Integer, Integer>, GlobalWindow> {

    @Override
    public void apply(Tuple5<Integer, Integer, Integer,Integer,Integer> keyed, GlobalWindow window, Iterable<PositionEvent> iterable, Collector<Accident> out)
            throws Exception {

              Accident accident = new Accident();
              PositionEvent lastElement = new PositionEvent();


              Iterator<PositionEvent> events = iterable.iterator();

              int time1 = events.next().f0;
              int count = 1;

              int counter = 1;
              while (events.hasNext()) {
                    count++;
                    lastElement = events.next();
                }

                if (count == 4) {
                    accident.setSTime(time1);
                    accident.setFTime(lastElement.f0);
                    accident.setVid(keyed.f0);
                    accident.setXway(keyed.f1);
                    accident.setSeg(keyed.f2);
                    accident.setDir(keyed.f3);
                    accident.setPos(keyed.f4);
                    out.collect(accident);
                  }
    }
}
