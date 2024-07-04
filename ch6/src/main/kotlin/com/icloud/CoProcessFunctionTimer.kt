package com.icloud

import com.icloud.extention.tuple
import com.icloud.model.SensorReading
import com.icloud.source.SensorSource
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

object CoProcessFunctionTimer {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        env.streamTimeCharacteristic = TimeCharacteristic.ProcessingTime

        val firstSwitches = env
            .fromCollection(
                listOf(
                    "sensor_2" tuple 10 * 1000L,
                    "sensor_7" tuple 60 * 1000L
                )
            )

        val readings = env
            .addSource(SensorSource())

        val forwardReadings = readings
            .connect(firstSwitches)
            .keyBy({ it.id }) { it.f0 }
            .process(ReadingFilter())

        forwardReadings.print()

        env.execute("Monitor sensor temperature.")
    }

    private class ReadingFilter :
        CoProcessFunction<SensorReading, Tuple2<String, Long>, SensorReading>() {

        private lateinit var forwardEnabled: ValueState<Boolean>
        private lateinit var disableTimer: ValueState<Long>

        override fun open(
            parameters: Configuration,
        ) {
            this.forwardEnabled =
                this.runtimeContext.getState(ValueStateDescriptor("filter_switch", Boolean::class.java))

            this.disableTimer =
                this.runtimeContext.getState(ValueStateDescriptor("timer", Long::class.java))
        }

        override fun processElement1(
            value: SensorReading,
            ctx: Context,
            out: Collector<SensorReading>,
        ) {
            if (forwardEnabled.value() == true) {
                out.collect(value)
            }
        }

        override fun processElement2(
            switch: Tuple2<String, Long>,
            ctx: Context,
            out: Collector<SensorReading>,
        ) {
            forwardEnabled.update(true)
            val timerTimestamp = ctx.timerService().currentProcessingTime() + switch.f1
            val currentTimerTimestamp = disableTimer.value() ?: 0L
            if (timerTimestamp > currentTimerTimestamp) {
                ctx.timerService().deleteProcessingTimeTimer(currentTimerTimestamp)
                ctx.timerService().registerProcessingTimeTimer(timerTimestamp)
                disableTimer.update(timerTimestamp)
            }
        }

        override fun onTimer(
            timestamp: Long,
            ctx: OnTimerContext,
            out: Collector<SensorReading>,
        ) {
            forwardEnabled.clear()
            disableTimer.clear()
        }
    }
}