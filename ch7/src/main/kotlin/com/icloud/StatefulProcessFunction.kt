package com.icloud

import com.icloud.extention.logger
import com.icloud.extention.valueNonNull
import com.icloud.extention.valueState
import com.icloud.model.SensorReading
import com.icloud.source.SensorSource
import com.icloud.timestamp.SensorTimeAssigner
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import java.time.Instant
import kotlin.math.absoluteValue


object StatefulProcessFunction {

    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        env.checkpointConfig.checkpointInterval = 10 * 1000

        env.config.autoWatermarkInterval = 1000

        val sensorData = env
            .addSource(SensorSource())
            .assignTimestampsAndWatermarks(SensorTimeAssigner())


        val keyedData = sensorData.keyBy { it.id }

        val alerts = keyedData
            .flatMap(TemperatureAlertFunction(threshold = 1.7))

        alerts.print()

        env.execute("Generate Temperature Alerts")
    }

    private data class TemperatureAlert(
        val sensorId: String,
        val temperature: Double,
        val tempDiff: Double,
    )

    private class TemperatureAlertFunction(
        private val threshold: Double,
    ) : RichFlatMapFunction<SensorReading, TemperatureAlert>() {

        companion object {
            private val LOG = TemperatureAlertFunction::class.logger()
        }

        private lateinit var lastTempState: ValueState<Double>

        override fun open(
            parameters: Configuration,
        ) {
            this.lastTempState = this.runtimeContext.getState("last_temp".valueState(Double::class))
                .also { LOG.info("[open] state [last_temp] initialized at {}", Instant.now()) }
        }

        override fun flatMap(
            value: SensorReading,
            out: Collector<TemperatureAlert>,
        ) {
            val lastTemp = this.lastTempState.valueNonNull()
            val tempDiff = (value.temperature - lastTemp).absoluteValue
            if (tempDiff > this.threshold) {
                out.collect(TemperatureAlert(value.id, value.temperature, tempDiff))
            }
            this.lastTempState.update(value.temperature)
        }
    }
}