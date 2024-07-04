package com.icloud

import com.icloud.extention.valueNonNull
import com.icloud.model.SensorReading
import com.icloud.source.SensorSource
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory
import java.time.Instant

object ProcessFunctionTimers {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        env.streamTimeCharacteristic = TimeCharacteristic.ProcessingTime

        val readings = env
            .addSource(SensorSource())

        val warnings = readings
            .keyBy { it.id }
            .process(TempIncreaseAlertFunction())

        warnings.print()

        env.execute("Monitor sensor temperatures.")

    }


    private class TempIncreaseAlertFunction : KeyedProcessFunction<String, SensorReading, String>() {
        companion object {
            private val LOG =
                LoggerFactory.getLogger(TempIncreaseAlertFunction::class.java)
        }


        /**
         * 마지막 으로 확인한 온도
         */
        private lateinit var lastTemp: ValueState<Double>

        /**
         * 마지막 활성화 된 타이머 타임스탬프
         */
        private lateinit var lastTimerTimestampState: ValueState<Long>

        override fun open(
            parameters: Configuration,
        ) {
            this.lastTemp =
                this.runtimeContext.getState(ValueStateDescriptor("last_temp", Double::class.java))
                    .also { LOG.info("[open] last_temp initialized at ${Instant.now()}") }
            this.lastTimerTimestampState =
                this.runtimeContext.getState(ValueStateDescriptor("last_timer_timestamp", Long::class.java))
                    .also { LOG.info("[open] last_timer_timestamp initialized at ${Instant.now()} ") }
        }

        override fun processElement(
            r: SensorReading,
            ctx: Context,
            out: Collector<String>,
        ) {
            val prevTemp = lastTemp.valueNonNull()
            lastTemp.update(r.temperature)
            val lastTimerTimestamp = lastTimerTimestampState.valueNonNull()

            if (prevTemp == .0 || r.temperature < prevTemp) {
                ctx.timerService().deleteProcessingTimeTimer(lastTimerTimestamp)
                lastTimerTimestampState.clear()
            } else if (r.temperature > prevTemp && lastTimerTimestamp == 0L) {
                val timerTs = ctx.timerService().currentProcessingTime() + 1_000L
                ctx.timerService().registerProcessingTimeTimer(timerTs)

                lastTimerTimestampState.update(timerTs)
            }
        }

        override fun onTimer(
            timestamp: Long,
            ctx: OnTimerContext,
            out: Collector<String>,
        ) {
            out.collect("Temperature of sensor '${ctx.currentKey}' monotonically increased 1 second.")
            lastTimerTimestampState.clear()
        }
    }
}