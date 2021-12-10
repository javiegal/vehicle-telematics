package master;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


/**
 * @author Ricardo María Longares Díez and Javier Gallego Gutiérrez
 */
public class VehicleTelematics {

    /**
     * Main method of the program
     *
     * @param args arguments provided to the program. The first one should be the input file and the second one the
     *             output folder.
     */
    public static void main(String[] args) {
        String inputFile = args[0];
        String outputFolder = args[1];

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(3);
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // The above commented method is deprecated. After Flink 1.12.0, time characteristic is set to EventTime by default

        // Read and parse input file
        DataStreamSource<String> source = env.readTextFile(inputFile);
        SingleOutputStreamOperator<PositionEvent> position = source.map(
                (MapFunction<String, PositionEvent>) s -> {
                    String[] fieldArray = s.split(",");
                    return new PositionEvent(Integer.parseInt(fieldArray[0]),
                            Integer.parseInt(fieldArray[1]), Integer.parseInt(fieldArray[2]),
                            Integer.parseInt(fieldArray[3]), Integer.parseInt(fieldArray[4]),
                            Integer.parseInt(fieldArray[5]), Integer.parseInt(fieldArray[6]),
                            Integer.parseInt(fieldArray[7]));
                }
        );

        // Average Speed Fines
        int windowGap = 1000; // Maximum number of seconds to wait for a new report from the same vehicle in the same highway and direction
        position
                .filter(pe -> pe.getSeg() >= VTConstants.INI_SEG && pe.getSeg() <= VTConstants.END_SEG)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PositionEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((pe, timeStamp) -> pe.getTime() * 1000))
                .keyBy(new KeySelector<PositionEvent, Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public Tuple3<Integer, Integer, Integer> getKey(PositionEvent pe) {
                        return new Tuple3<Integer, Integer, Integer>(pe.getVid(), pe.getXway(), pe.getDir());
                    }
                })
                .window(EventTimeSessionWindows.withGap(Time.seconds(windowGap)))
                .apply(new AvgSpeedControl(windowGap))
                .writeAsCsv(outputFolder + "/avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // Speed Fines
        position
                .filter(pe -> pe.getSpd() > VTConstants.MAX_SPEED)
                .map(new MapFunction<PositionEvent, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> () {
                        @Override
                        public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> map (PositionEvent pe){
                            return new Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>(pe.getTime(),
                                    pe.getVid(), pe.getXway(),pe.getSeg(),pe.getDir(),pe.getSpd());
                        }
                })
                .writeAsCsv(outputFolder + "/speedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // Accident Report
        position
                .filter(pe -> pe.getSpd() == 0)
                .keyBy(new KeySelector<PositionEvent, Tuple5<Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple5<Integer, Integer, Integer, Integer, Integer> getKey(PositionEvent pe) {
                        return new Tuple5<Integer, Integer, Integer, Integer, Integer>(pe.getVid(), pe.getXway(),
                                pe.getSeg(), pe.getDir(), pe.getPos());
                    }
                })
                .countWindow(VTConstants.REPORTS_ACCIDENT, 1)
                .apply(new AccidentReporter())
                .writeAsCsv(outputFolder + "/accidents.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // Execute program
        try {
            env.execute("Vehicle Telematics");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
