package com.icloud

import com.icloud.model.SensorReading
import com.icloud.source.SensorSource
import com.icloud.timestamp.SensorTimeAssigner
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag

object SideOutputs {
    @JvmStatic
    fun main(args: Array<String>) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        env.checkpointConfig.checkpointInterval = 10 * 1000

        env.config.autoWatermarkInterval = 1000L

        val readings = env
            .addSource(SensorSource())
            .setParallelism(5)
            .assignTimestampsAndWatermarks(SensorTimeAssigner())
            .setParallelism(5)
            .uid("Read Sensor")

        val monitorReadings = readings
            .process(FreezingMonitor())
            .setParallelism(3)
            .uid("Freezing Monitor")

        monitorReadings.getSideOutput(object : OutputTag<String>("freezing-alarms") {})
            .print("SIDE")
            .setParallelism(1)
            .uid("Print Side Output")

        monitorReadings
            .print("MAIN")
            .setParallelism(8)
            .uid("Print Main Output")

        env.execute()
    }

    private class FreezingMonitor : ProcessFunction<SensorReading, SensorReading>() {

        private val freezingAlarmOutput: OutputTag<String> by lazy {
            object : OutputTag<String>("freezing-alarms") {}
        }

        override fun processElement(
            r: SensorReading,
            ctx: Context,
            out: Collector<SensorReading>,
        ) {
            if (r.temperature < 32.0) {
                ctx.output(freezingAlarmOutput, "Freezing Alarm for ${r.id}")
            }

            out.collect(r)
        }

    }
}