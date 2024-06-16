package com.icloud

import model.Alert
import model.SensorReading
import model.SmokeLevel
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.util.Collector
import source.SensorSource
import source.SmokeLevelSource
import timestamp.SensorTimeAssigner

object MultiStreamTransformations {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        env.config.autoWatermarkInterval = 1_000L

        val tempReadings =
            env.addSource(SensorSource())
                .assignTimestampsAndWatermarks(SensorTimeAssigner())

        val smokeReadings =
            env.addSource(SmokeLevelSource())
                .setParallelism(1)

        val keyed =
            tempReadings.keyBy { it.id }


        val alerts = keyed
            .connect(smokeReadings.broadcast())
            .flatMap(RaiseAlertFlatMap())

        alerts.print()

        env.execute("Multi-Stream Transformations Example")
    }

    private class RaiseAlertFlatMap
        : CoFlatMapFunction<SensorReading, SmokeLevel, Alert> {

        private var smokeLevel: SmokeLevel = SmokeLevel.LOW

        override fun flatMap1(
            value: SensorReading,
            out: Collector<Alert>,
        ) {
            if (this.smokeLevel == SmokeLevel.HIGH &&
                value.temperature > 100
            ) {
                out.collect(Alert("Risk of fire!", value.timestamp))
            }
        }

        override fun flatMap2(
            value: SmokeLevel,
            out: Collector<Alert>,
        ) {
            this.smokeLevel = value
        }

    }
}