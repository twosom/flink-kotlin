package com.icloud

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class ThirtySecondsWindows<InputT> :
    WindowAssigner<InputT, TimeWindow>() {
    companion object {
        /**
         * Thirty Seconds
         */
        private const val WINDOW_SIZE = 30 * 1_000L
    }

    override fun assignWindows(
        element: InputT,
        timestamp: Long,
        context: WindowAssignerContext,
    ): Collection<TimeWindow> {
        val startTime = timestamp - (timestamp % WINDOW_SIZE)
        val endTime = startTime + WINDOW_SIZE
        return listOf(TimeWindow(startTime, endTime))
    }

    @Suppress("UNCHECKED_CAST")
    override fun getDefaultTrigger(
        env: StreamExecutionEnvironment,
    ): Trigger<InputT, TimeWindow> =
        EventTimeTrigger.create() as Trigger<InputT, TimeWindow>

    override fun getWindowSerializer(
        executionConfig: ExecutionConfig,
    ): TypeSerializer<TimeWindow> = TimeWindow.Serializer()

    override fun isEventTime() = true

    override fun toString(): String = "ThirtySecondsWindows()"
}