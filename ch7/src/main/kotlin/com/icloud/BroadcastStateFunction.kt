package com.icloud

import com.icloud.extention.logger
import com.icloud.extention.mapState
import com.icloud.extention.valueNonNull
import com.icloud.extention.valueState
import com.icloud.model.SensorReading
import com.icloud.source.SensorSource
import com.icloud.timestamp.SensorTimeAssigner
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector
import kotlin.math.absoluteValue

object BroadcastStateFunction {

    data class ThresholdUpdate(
        val id: String,
        val threshold: Double,
    )

    val broadcastStateDescriptor: MapStateDescriptor<String, Double> =
        "thresholds".mapState(String::class, Double::class)

    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        env.checkpointConfig.checkpointInterval = 10 * 1000

        env.config.autoWatermarkInterval = 1000

        val sensorData = env
            .addSource(SensorSource())
            .assignTimestampsAndWatermarks(SensorTimeAssigner())

        val thresholds: DataStream<ThresholdUpdate> = env.fromElements(
            ThresholdUpdate("sensor_1", 5.0),
            ThresholdUpdate("sensor_2", 0.9),
            ThresholdUpdate("sensor_3", 0.5),
            ThresholdUpdate("sensor_1", 1.2),  // update threshold for sensor_1
            ThresholdUpdate("sensor_3", 0.0)  // disable threshold for sensor_3
        )


        val broadcastThreshold: BroadcastStream<ThresholdUpdate> = thresholds
            .broadcast(broadcastStateDescriptor)

        val alerts = sensorData.keyBy { it.id }
            .connect(broadcastThreshold)
            .process(UpdatableTemperatureAlertFunction())

        alerts.print()

        env.execute("Broadcast State Function Example")
    }

    private data class TemperatureAlert(
        val sensorId: String,
        val temperature: Double,
        val tempDiff: Double,
    )

    private class UpdatableTemperatureAlertFunction
        : KeyedBroadcastProcessFunction<String, SensorReading, ThresholdUpdate, TemperatureAlert>() {

        private lateinit var lastTempState: ValueState<Double>

        companion object {
            private val LOG = UpdatableTemperatureAlertFunction::class.logger()
        }

        override fun open(
            parameters: Configuration,
        ) {
            this.lastTempState = this.runtimeContext.getState("last_temp".valueState(Double::class))
                .also { LOG.info("state [last_temp] initialized...") }
        }

        override fun processBroadcastElement(
            update: ThresholdUpdate,
            ctx: Context,
            out: Collector<TemperatureAlert>,
        ) {
            ctx.applyToKeyedState("last_temp".valueState(Double::class)) { key, state ->
                LOG.info("key = {}, value = {}", key, state.valueNonNull())
            }
            val thresholds = ctx.getBroadcastState(broadcastStateDescriptor)
                .also { LOG.info("[processBroadcastElement] load from {}", this.runtimeContext.indexOfThisSubtask) }

            if (update.threshold != .0) {
                thresholds.put(update.id, update.threshold)
            } else {
                thresholds.remove(update.id)
            }
        }

        override fun processElement(
            reading: SensorReading,
            ctx: ReadOnlyContext,
            out: Collector<TemperatureAlert>,
        ) {
            val thresholds = ctx.getBroadcastState(broadcastStateDescriptor)

            if (thresholds.contains(reading.id)) {
                val sensorThreshold: Double = thresholds.get(reading.id)

                val lastTemp = this.lastTempState.valueNonNull()

                val tempDiff = (reading.temperature - lastTemp).absoluteValue

                if (tempDiff > sensorThreshold) {
                    out.collect(TemperatureAlert(reading.id, reading.temperature, tempDiff))
                }
            }

            this.lastTempState.update(reading.temperature)
        }

    }
}