package master;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Class that reports accidents. It implements the window function interface.
 */
public class AccidentReporter implements WindowFunction<PositionEvent, Accident,
        Tuple5<Integer, Integer, Integer, Integer, Integer>, GlobalWindow> {

    @Override
    public void apply(Tuple5<Integer, Integer, Integer, Integer, Integer> keyed, GlobalWindow window,
                      Iterable<PositionEvent> iterable, Collector<Accident> out) {

        Iterator<PositionEvent> peIt = iterable.iterator();
        PositionEvent pe = peIt.next();
        int ini = pe.getTime();
        int count = 1;

        for (; peIt.hasNext(); pe = peIt.next())
            count++;


        if (count == 4)
            out.collect(new Accident(ini, pe.getTime(), keyed.getField(0), keyed.getField(1),
                    keyed.getField(2), keyed.getField(3), keyed.getField(4)));
    }
}
