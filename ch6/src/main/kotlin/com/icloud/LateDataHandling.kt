package com.icloud

import com.icloud.extention.tuple
import com.icloud.extention.valueState
import com.icloud.model.SensorReading
import com.icloud.source.SensorSource
import com.icloud.timestamp.SensorTimeAssigner
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.api.java.tuple.Tuple4
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag
import java.security.SecureRandom
import java.util.*


private val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment()
    .apply { checkpointConfig.checkpointInterval = 10 * 1000 }
    .apply { config.autoWatermarkInterval = 500L }
    .also { println("env init...") }

private fun readData(env: StreamExecutionEnvironment): SingleOutputStreamOperator<SensorReading> =
    env
        .addSource(SensorSource())
        .map(TimestampShuffler(7 * 1000))
        .assignTimestampsAndWatermarks(SensorTimeAssigner())

private val lateReadingOutputTag: OutputTag<SensorReading> =
    object : OutputTag<SensorReading>("late-readings") {}


class TimestampShuffler(
    private val maxRandomOffset: Int,
) : MapFunction<SensorReading, SensorReading> {

    private val rand: Random by lazy { SecureRandom() }

    override fun map(
        value: SensorReading,
    ): SensorReading =
        (value.timestamp + this.rand.nextInt(this.maxRandomOffset))
            .let { value.copy(timestamp = it) }
}

/**
 * Re-Send Late Data
 */
object ResendLateData {
    @JvmStatic
    fun main(args: Array<String>) {
        val readings = readData(env)


        val countPer10secs = readings.keyBy { it.id }
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .sideOutputLateData(lateReadingOutputTag)
            .process(object : ProcessWindowFunction<SensorReading, Tuple3<String, Long, Int>, String, TimeWindow>() {
                override fun process(
                    key: String,
                    context: Context,
                    elements: Iterable<SensorReading>,
                    out: Collector<Tuple3<String, Long, Int>>,
                ) {
                    val cnt = elements.count()
                    out.collect(key tuple context.window().end tuple cnt)
                }
            })

        countPer10secs
            .print("MAIN_DATA")

        countPer10secs
            .getSideOutput(lateReadingOutputTag)
            .print("LATE_DATA")

        env.execute("Resend Late Data")
    }


}


object FilterLateData {
    @JvmStatic
    fun main(args: Array<String>) {
        val readings = readData(env)

        val filteredReadings = readings
            .keyBy { it.id }
            .process(LateDataFilterProcess())

        filteredReadings.print("MAIN_DATA")

        filteredReadings
            .getSideOutput(lateReadingOutputTag)
            .print("LATE_DATA")


        env.execute("Filtering Late Data")
    }

    private class LateDataFilterProcess
        : ProcessFunction<SensorReading, SensorReading>() {
        private lateinit var lateDataCountState: ValueState<Long>
        private lateinit var isTimerSetState: ValueState<Boolean>

        override fun open(
            parameters: Configuration,
        ) {
            this.lateDataCountState = this.runtimeContext.getState("late_data_count".valueState(Long::class))
            this.isTimerSetState = this.runtimeContext.getState("timer_set".valueState(Boolean::class))
        }

        override fun processElement(
            value: SensorReading,
            ctx: Context,
            out: Collector<SensorReading>,
        ) {
            if (value.timestamp < ctx.timerService().currentWatermark()) {
                val currentLateDataCount = this.lateDataCountState.value() ?: 0
                val isTimerSet = this.isTimerSetState.value() ?: false
                ctx.output(lateReadingOutputTag, value)
                this.lateDataCountState.update(currentLateDataCount + 1)
                if (!isTimerSet) {
                    registerTimer(ctx)
                }
            }
        }

        override fun onTimer(
            timestamp: Long,
            ctx: OnTimerContext,
            out: Collector<SensorReading>,
        ) {
            val currentLateDataCount = this.lateDataCountState.value() ?: 0
            println("Current Late Data Count = $currentLateDataCount")
            // looping timer...
            registerTimer(ctx)
        }

        private fun registerTimer(ctx: Context) {
            val currentTime = ctx.timerService().currentProcessingTime()
            ctx.timerService().registerProcessingTimeTimer(currentTime + 30_000)
        }
    }
}

object UpdateLateData {
    @JvmStatic
    fun main(args: Array<String>) {
        val readings = readData(env)

        readings
            .keyBy { it.id }
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .allowedLateness(Time.seconds(5))
            .process(UpdateWindowCountFunction())
            .print()


        env.execute("Update Late Data")
    }

    private class UpdateWindowCountFunction
        : ProcessWindowFunction<SensorReading, Tuple4<String, Long, Int, String>, String, TimeWindow>() {

        private lateinit var isUpdateState: ValueState<Boolean>

        override fun open(parameters: Configuration?) {
            this.isUpdateState = this.runtimeContext.getState("update".valueState(Boolean::class))
        }

        override fun process(
            key: String,
            context: Context,
            elements: Iterable<SensorReading>,
            out: Collector<Tuple4<String, Long, Int, String>>,
        ) {
            val cnt = elements.count()

            val isUpdate = this.isUpdateState.value() ?: false

            if (!isUpdate) {
                out.collect(
                    key tuple context.window().end tuple cnt tuple "first"
                )

                isUpdateState.update(true)
            } else {
                out.collect(
                    key tuple context.window().end tuple cnt tuple "update"
                )
            }
        }

    }
}

