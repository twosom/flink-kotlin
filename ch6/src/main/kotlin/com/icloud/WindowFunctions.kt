package com.icloud

import com.icloud.extention.max
import com.icloud.extention.min
import com.icloud.extention.reduce
import com.icloud.extention.tuple
import com.icloud.model.SensorReading
import com.icloud.source.SensorSource
import com.icloud.timestamp.SensorTimeAssigner
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.typeinfo.TypeHint
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object ReduceFunctionExample {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        env.checkpointConfig.checkpointInterval = Time.seconds(10).toMilliseconds()

        env.config.autoWatermarkInterval = Time.seconds(1).toMilliseconds()

        val sensorData: DataStream<SensorReading> = env
            .addSource(SensorSource())
            .assignTimestampsAndWatermarks(SensorTimeAssigner())

        val minTempPerWindow = sensorData
            .map { it.id tuple it.temperature }
            .returns(object : TypeHint<Tuple2<String, Double>>() {})
            .keyBy { it.f0 }
            .window(TumblingEventTimeWindows.of(Time.seconds(15)))
            .reduce { r1, r2 ->
                r1.f0 tuple r1.f1.min(r2.f1)
            }

        minTempPerWindow.print("MinTempPerWindow")

        env.execute("MinTempPerWindow Application")

    }
}

object AggregateFunctionExample {

    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        env.checkpointConfig.checkpointInterval = Time.seconds(10).toMilliseconds()

        env.config.autoWatermarkInterval = Time.seconds(1).toMilliseconds()

        val sensorData = env
            .addSource(SensorSource())
            .assignTimestampsAndWatermarks(SensorTimeAssigner())

        val avgTempPerWindow = sensorData
            .map { it.id tuple it.temperature }
            .returns(object : TypeHint<Tuple2<String, Double>>() {})
            .keyBy { it.f0 }
            .window(TumblingEventTimeWindows.of(Time.seconds(15)))
            .aggregate(AvgTempFunction())

        avgTempPerWindow.print("AvgTempPerWindow")

        env.execute("AvgTempPerWindow Application")
    }

    class AvgTempFunction :
        AggregateFunction<
                Tuple2<String, Double>,
                Accum,
                Tuple2<String, Double>
                > {
        override fun createAccumulator() = Accum.empty()

        override fun add(
            value: Tuple2<String, Double>,
            accumulator: Accum,
        ) = accumulator.copy(key = value.f0, sum = value.f1 + accumulator.sum, count = accumulator.count + 1)

        override fun merge(
            a: Accum,
            b: Accum,
        ) = a.copy(sum = a.sum + b.sum, count = a.count + b.count)

        override fun getResult(
            accumulator: Accum,
        ): Tuple2<String, Double> =
            accumulator.key tuple (accumulator.sum / accumulator.count)
    }

    data class Accum(
        val key: String,
        val sum: Double,
        val count: Int,
    ) {
        companion object {
            fun empty(): Accum = Accum("", .0, 0)
        }
    }
}

data class MinMaxTemp(
    val id: String,
    val min: Double,
    val max: Double,
    val endTs: Long,
)

object ProcessWindowFunctionExample {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        env.checkpointConfig.checkpointInterval = Time.seconds(1).toMilliseconds()

        env.config.autoWatermarkInterval = Time.seconds(1).toMilliseconds()

        val sensorData = env
            .addSource(SensorSource())
            .assignTimestampsAndWatermarks(SensorTimeAssigner())

        val minMaxTempPerWindow = sensorData
            .keyBy { it.id }
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .process(HighAndLowTempProcessFunction())

        minMaxTempPerWindow.print("MinMaxTempPerWindow")

        env.execute("MinMaxTempPerWindow Application")
    }

    class HighAndLowTempProcessFunction
        : ProcessWindowFunction<SensorReading, MinMaxTemp, String, TimeWindow>() {

        override fun process(
            key: String,
            context: Context,
            elements: Iterable<SensorReading>,
            out: Collector<MinMaxTemp>,
        ) {
            val temps = elements.map { it.temperature }

            val windowEnd = context.window().end
            out.collect(MinMaxTemp(key, temps.min(), temps.max(), windowEnd))
        }

    }

}

object ReducingWithWindowFunction {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        env.checkpointConfig.checkpointInterval = Time.seconds(1).toMilliseconds()

        env.config.autoWatermarkInterval = Time.seconds(1).toMilliseconds()

        val sensorData = env
            .addSource(SensorSource())
            .assignTimestampsAndWatermarks(SensorTimeAssigner())


        val minMaxTempByReducingWithWindowFunction = sensorData
            .map { Holder(it.id, it.temperature, it.temperature) }
            .keyBy { it.key }
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .reduce(AssignWindowEndProcessFunction()) { r1, r2 ->
                r1.copy(
                    min = r1.min.min(r2.min),
                    max = r1.max.max(r2.max)
                )
            }

        minMaxTempByReducingWithWindowFunction.print("MinMaxTemp_ByReducingWithWindowFunction")

        env.execute("MinMaxTemp By Reducing With Window Function")
    }

    data class Holder(
        val key: String,
        val min: Double,
        val max: Double,
    )

    class AssignWindowEndProcessFunction
        : ProcessWindowFunction<Holder, MinMaxTemp, String, TimeWindow>() {

        override fun process(
            key: String,
            context: Context,
            elements: Iterable<Holder>,
            out: Collector<MinMaxTemp>,
        ) {
            val (_, min, max) = elements.first()
            val windowEnd = context.window().end
            out.collect(MinMaxTemp(key, min, max, windowEnd))
        }

    }
}