package flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.AverageAccumulator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;

/**
 * @author diego.ruiz
 */
public class CarAdsDedupJob {

    public static void main(String args[]) throws Exception {

        // job config
        final ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.getConfig().setParallelism(params.getInt("parallelism", 1));
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.registerType(CarAd.class);
        final String pathname = params.get("pathname", "src/resources/cars.json");

        // stream from file
        KeyedStream<CarAd, Tuple> stream = env.readTextFile(pathname)
                .map(new CarAdMapper())
                .keyBy("make","model","year","doors","fuel","transmission","color");

        // print the duplicates
        stream.flatMap(new DedupFilterRetain())
                .rebalance()
                .print()
                .setParallelism(env.getConfig().getParallelism());

        // remove the duplicates
        stream.flatMap(new DedupFilterDismiss())
                .rebalance()
                .process(new ProcessStatsFunction())
                .addSink(new CarAdSink()).name("car-ads-dedup")
                .setParallelism(env.getConfig().getParallelism());

        JobStats jobStats = new JobStats();
        jobStats.startTime = Instant.now();
        JobExecutionResult execResult = env.execute("Car Ads stream processor");
        jobStats.endTime = Instant.now();

        System.out.println("Duplicates found: " + execResult.getAccumulatorResult("dup-counter"));
        System.out.println("Statistics" + execResult.getAllAccumulatorResults());
        System.out.println("Total job time: " + Duration.between(jobStats.startTime, jobStats.endTime));
    }
}

/**
 * Maps every line in the JSON file to our CarAd POJO.
 */
@SuppressWarnings("serial")
class CarAdMapper implements MapFunction<String, CarAd> {
    ObjectMapper objectMapper = new ObjectMapper();
    @Override public CarAd map(String line) throws Exception {
        return objectMapper.readValue(line, CarAd.class);
    }
}

/**
 * Lookups if the operator has already seen a key.
 * 
 * Rationale: The stateful mapper uses Flink's state-abstraction to store if it has already seen this key or not. 
 * This gets benefit from the Flink's fault tolerance mechanism, because state gets automatically checkpointed.
 * Filtering duplicates over an infinite stream will eventually fail if key space is larger than available storage,
 * unless we define a time window/event trigger for the lookup.
 * 
 * Defines a collect() operation to decide the retention for the matches.
 */
@SuppressWarnings("serial")
abstract class DedupFilter extends RichFlatMapFunction<CarAd, CarAd> {
    final ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen", Boolean.class, Boolean.FALSE);
    private ValueState<Boolean> operatorState;
    @Override public void open(Configuration configuration) throws Exception {
        operatorState = this.getRuntimeContext().getState(descriptor);
    }
    @Override public void flatMap(CarAd value, Collector<CarAd> out) throws Exception {
        collect(value, out, operatorState);
    }
    public abstract void collect(CarAd value, Collector<CarAd> out, ValueState<Boolean> operatorState) throws Exception;
}

/**
 * This flavor of DedupFilter returns to the output collector the records that are duplicated.
 * @see DedupFilter
 */
@SuppressWarnings("serial")
class DedupFilterRetain extends DedupFilter {
    private IntCounter dupCounter = new IntCounter();
    @Override public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        getRuntimeContext().addAccumulator("dup-counter", this.dupCounter);
    }
    @Override public void collect(CarAd value, Collector<CarAd> out, ValueState<Boolean> operatorState) throws Exception {
        if (!operatorState.value()) {
            operatorState.update(true);
        } else {
            out.collect(value); //we have seen this element already! -> report it!
            this.dupCounter.add(1);
        }
    }
}

/**
 * This flavor of DedupFilter returns to the output collector the records that are distinct.
 * @see DedupFilter
 */
@SuppressWarnings("serial")
class DedupFilterDismiss extends DedupFilter {
    @Override public void collect(CarAd item, Collector<CarAd> out, ValueState<Boolean> operatorState) throws Exception {
        if (!operatorState.value()) {
            operatorState.update(true);
            out.collect(item); //we only want the unseen yet (duplicates are dismissed)
        }
    }
}

/**
 * Processing function to perform aggregations/statistics.
 */
@SuppressWarnings("serial")
class ProcessStatsFunction extends ProcessFunction<CarAd,CarAd> {
    private AverageAccumulator avgMileage = new AverageAccumulator();
    private AverageAccumulator avgPrice = new AverageAccumulator();
    @Override public void open(Configuration parameters) throws Exception {
        getRuntimeContext().addAccumulator("avg-mileage", this.avgMileage);
        getRuntimeContext().addAccumulator("avg-price", this.avgPrice);
    }
    @Override public void processElement(CarAd item, ProcessFunction<CarAd, CarAd>.Context ctx, Collector<CarAd> out) throws Exception {
        this.avgMileage.add(Double.parseDouble(item.mileage));
        this.avgPrice.add(Double.parseDouble(item.price));
        out.collect(item);
    }
}

/**
 * Sink for our munged data stream.
 * This could be for example a KafkaSink, where the Semantics policy of Kafka can apply.
 */
@SuppressWarnings("serial")
class CarAdSink implements SinkFunction<CarAd> {
    @Override public void invoke(CarAd carAd) throws Exception {
        //System.out.println("UNIQUE "+carAd);
    }
}

class JobStats {
    public Instant startTime, endTime;
}
