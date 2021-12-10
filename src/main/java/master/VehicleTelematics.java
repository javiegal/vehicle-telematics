package master;
import master.Accident;
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
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.Iterator;
/*
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
 */



public class VehicleTelematics {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        String input = args[0];
        String output = args[1];

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

        /* Needed to write a CSV file not using deprecated methods
        OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("avgspeedfines")
                .withPartSuffix(".csv")
                .build();

        env.enableCheckpointing(600000);
        final StreamingFileSink<String> sinkAvg = StreamingFileSink
            .forRowFormat(new Path(output), new SimpleStringEncoder<String>("UTF-8"))
            .withOutputFileConfig(config)
            .build();
         */
         //Average Speed Fines
        position
                .filter(pe -> pe.getSeg() >= 52 && pe.getSeg() <= 56)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PositionEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timeStamp) -> event.getTime() * 1000))
                // Make sure it has to be forMonotonousTimeStamps()
                .keyBy(new KeySelector<PositionEvent, Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public Tuple3<Integer, Integer, Integer> getKey(PositionEvent pe) throws Exception {
                        return new Tuple3<Integer, Integer, Integer>(pe.getVid(), pe.getXway(), pe.getDir());
                    }
                })
                .window(EventTimeSessionWindows.withGap(Time.seconds(100))) // Appropriate value for the gap? With 30 or lesss it does not return the same output
                .apply(new AvgSpeedWindow())
                .writeAsCsv(output + "/avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
                // .map(AvgSpeedFine::toString).addSink(sinkAvg); No deprecated way to do it (problems with checkpointing)
        //Speed Fines
        position
                .filter(pe -> pe.getSpd() > 90)
                .map(new MapFunction<PositionEvent,SpeedFine>() {
                  SpeedFine speedFine = new SpeedFine();
                    @Override
                    public SpeedFine map(PositionEvent pe) throws Exception {
                        speedFine.setTime(pe.getTime());
                        speedFine.setVid(pe.getVid());
                        speedFine.setXway(pe.getXway());
                        speedFine.setSeg(pe.getSeg());
                        speedFine.setDir(pe.getDir());
                        speedFine.setSpd(pe.getSpd());
                        return speedFine;
                    }
                })
                .writeAsCsv(output + "/speedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        //Accident Report

        position
                .filter(pe -> pe.getSpd() == 0).setParallelism(1)
                .keyBy(new KeySelector<PositionEvent, Tuple5<Integer, Integer, Integer,Integer,Integer>>() {
                    @Override
                    public Tuple5<Integer, Integer, Integer,Integer,Integer> getKey(PositionEvent pe) throws Exception {
                        return new Tuple5<Integer, Integer, Integer,Integer,Integer>(pe.getVid(),pe.getXway(),pe.getSeg(), pe.getDir(), pe.getPos());
                    }
                })
                .countWindow(4, 1)
                .apply(new RepOfAccident())
                .writeAsCsv(output + "/accidents.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        // execute program
        try {
            env.execute("Vehicle Telematics");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
