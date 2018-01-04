package main.java;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class FlinkEventTimeProcessor {

    public static void main(String[] args) throws Exception {

        args = new String[]{
                "-state.checkpoints.dir", "file:///"+ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH+"flink"+File.separator+"eventsCheckpoints",
        };

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameters = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameters);

        // Fault tolerance config
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(10, TimeUnit.SECONDS)));
        env.setStateBackend(new FsStateBackend("file:///"+ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH+"flink"+File.separator+"eventsFS"));

        DataStream<Event> eventStream = env.addSource(new EventGenerator());
        eventStream.assignTimestampsAndWatermarks(new TimestampExtractor())
                .keyBy("someData")
                .process(new SortFunction())
                .print().name("print-sink");

        JobExecutionResult result = env.execute("Run Stream smoother");
    }

    public static class SortFunction extends ProcessFunction<Event, Event> {
        private ValueState<PriorityQueue<Event>> queueState = null;
        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<PriorityQueue<Event>> descriptor = new ValueStateDescriptor<>(
                    "sorted-events", // state name
                    TypeInformation.of(new TypeHint<PriorityQueue<Event>>(){}) // type information of state
            );
            queueState = getRuntimeContext().getState(descriptor);
        }
        @Override
        public void processElement(Event event, Context context, Collector<Event> out) throws Exception {
            TimerService timerService = context.timerService();
            if (context.timestamp() > timerService.currentWatermark()) {
                PriorityQueue<Event> queue = queueState.value();
                if (queue == null) {
                    queue = new PriorityQueue<>(10);
                }
                queue.add(event);
                queueState.update(queue);
                timerService.registerEventTimeTimer(event.time);
            }
        }
        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Event> out) throws Exception {
            PriorityQueue<Event> queue = queueState.value();
            Long watermark = context.timerService().currentWatermark();
            Event head = queue.peek();
            while (head != null && head.time <= watermark) {
                out.collect(head);
                queue.remove(head);
                head = queue.peek();
            }
        }
    }

    public static class TimestampExtractor implements AssignerWithPunctuatedWatermarks<Event> {
        private final long maxOutOfOrderness = 30_000L; // ET randomization goes from 00_000 to 29_999
        @Override
        public long extractTimestamp(Event event, long previousElementTimestamp) {
            return event.time;
        }
        @Override
        public Watermark checkAndGetNextWatermark(Event event, long extractedTimestamp) {
            // simply emit a watermark with every event
            return new Watermark(extractedTimestamp - maxOutOfOrderness);
        }
    }

    public static class Event implements Serializable, Comparable<Event> {
        public String someData;
        public long time;
        @Override
        public String toString() {
            return "Event(" + time + ", " + someData + ')';
        }
        @Override
        public int compareTo(Event other) {
            return Long.compare(this.time, other.time);
        }
    }

    public static class EventGenerator implements ParallelSourceFunction<Event>, ListCheckpointed<Long> {
        private static final long serialVersionUID = 9159457797902106334L;
        private boolean running = true;
        public static final long OutOfOrderness = 15000L;
        public long counter = OutOfOrderness;

        public void run(SourceContext<Event> sourceContext) throws Exception {
            StringBuffer buf = new StringBuffer();
            while (running) {
                Event evt = new Event();
                long random = ThreadLocalRandom.current().nextLong(-OutOfOrderness, OutOfOrderness);
                evt.time = counter + random;
                buf.append("original index:").append(counter).append(" time: ").append(evt.time);
                evt.someData = buf.toString();
                buf.setLength(0);
                synchronized (sourceContext.getCheckpointLock()) {
                    sourceContext.collect(evt);
                    counter++;
                }
                if(counter % 1_000L == 0) {
                    Thread.sleep(10);
                }
            }
        }

        public void cancel() {
            running = false;
        }

        @Override
        public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(counter);
        }

        @Override
        public void restoreState(List<Long> state) throws Exception {
            counter = state.get(0);
        }
    }
}
