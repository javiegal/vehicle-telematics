package master;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;



public class VehicleTelematics {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        String input = args[0];
        String output = args[1];

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(3);

        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // Above method is deprecated after Flink 1.12.0: time characteristic set to EventTime by default

        DataStreamSource<String> source = env.readTextFile(input);
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
        position
                .filter(pe -> pe.getSeg() >= 52 && pe.getSeg() <= 56)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PositionEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((pe, timeStamp) -> pe.getTime() * 1000))
                .keyBy(new KeySelector<PositionEvent, Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public Tuple3<Integer, Integer, Integer> getKey(PositionEvent pe) throws Exception {
                        return new Tuple3<Integer, Integer, Integer>(pe.getVid(), pe.getXway(), pe.getDir());
                    }
                })
                .window(EventTimeSessionWindows.withGap(Time.seconds(100))) // Appropriate value for the gap? With 30 or lesss it does not return the same output
                .apply(new AvgSpeedControl())
                .writeAsCsv(output + "/avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        // Speed Fines
        position
                .filter(pe -> pe.getSpd() > 90)
                .map((MapFunction<PositionEvent, SpeedFine>) pe -> new SpeedFine(pe.getTime(), pe.getVid(),
                        pe.getXway(), pe.getSeg(), pe.getDir(), pe.getSpd()))
                .writeAsCsv(output + "/speedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // Accident Report
        position
                .filter(pe -> pe.getSpd() == 0)
                .keyBy(new KeySelector<PositionEvent, Tuple5<Integer, Integer, Integer,Integer,Integer>>() {
                    @Override
                    public Tuple5<Integer, Integer, Integer,Integer,Integer> getKey(PositionEvent pe) throws Exception {
                        return new Tuple5<Integer, Integer, Integer,Integer,Integer>(pe.getVid(), pe.getXway(),
                                pe.getSeg(), pe.getDir(), pe.getPos());
                    }
                })
                .countWindow(4, 1)
                .apply(new AccidentReporter())
                .writeAsCsv(output + "/accidents.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);


        // Execute program
        try {
            env.execute("Vehicle Telematics");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
