package com.icloud

import com.icloud.extention.logger
import com.icloud.extention.valueNonNull
import com.icloud.extention.valueState
import com.icloud.model.SensorReading
import com.icloud.source.SensorSource
import com.icloud.timestamp.SensorTimeAssigner
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import kotlin.math.absoluteValue

object SelfCleaningExample {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        env.checkpointConfig.checkpointInterval = 10 * 1000L

        env.config.autoWatermarkInterval = 1000

        val sensorData = env
            .addSource(SensorSource())
            .assignTimestampsAndWatermarks(SensorTimeAssigner())

        val keyedSensorData = sensorData
            .keyBy { it.id }

        val alerts = keyedSensorData
            .process(SelfCleaningTemperatureAlertFunction(threshold = 1.5))

        alerts.print()

        env.execute("Self Cleaning Example")

    }

    private data class TemperatureAlert(
        val sensorId: String,
        val temperature: Double,
        val tempDiff: Double,
    )

    private class SelfCleaningTemperatureAlertFunction(
        private val threshold: Double,
    ) : KeyedProcessFunction<String, SensorReading, TemperatureAlert>() {

        companion object {
            private val LOG = SelfCleaningTemperatureAlertFunction::class.logger()
            private const val HOUR = 3600 * 1000
        }

        private val tempDiffCounter = IntCounter()

        private lateinit var lastTempState: ValueState<Double>
        private lateinit var lastTimerState: ValueState<Long>


        override fun open(
            parameters: Configuration,
        ) = with(this.runtimeContext) {
            lastTempState = getState("last_temp".valueState(Double::class))
            lastTimerState = getState("last_timestamp".valueState(Long::class))
            this.addAccumulator("TEMP_DIFF_COUNTER", tempDiffCounter)
        }

        override fun processElement(
            value: SensorReading,
            ctx: Context,
            out: Collector<TemperatureAlert>,
        ) {
            val newTimer = ctx.timestamp() + HOUR
            val currentTimer = this.lastTimerState.valueNonNull()

            with(ctx.timerService()) {
                deleteEventTimeTimer(currentTimer)
                registerEventTimeTimer(newTimer)
            }.also { this.lastTimerState.update(newTimer) }

            val lastTemp = this.lastTempState.valueNonNull()
            val tempDiff = (value.temperature - lastTemp).absoluteValue
            if (tempDiff > this.threshold) {
                out.collect(TemperatureAlert(value.id, value.temperature, tempDiff))
                    .also { this.tempDiffCounter.add(1) }
            }
            this.lastTempState.update(value.temperature)
        }

        override fun onTimer(
            timestamp: Long,
            ctx: OnTimerContext,
            out: Collector<TemperatureAlert>,
        ) {
            this.lastTempState.clear()
                .also { LOG.info("[onTimer] state [last_temp] cleared at {}", ctx.timestamp()) }
            this.lastTimerState.clear()
                .also { LOG.info("[onTimer] state [last_timestamp] cleared at {}", ctx.timestamp()) }
        }
    }
}