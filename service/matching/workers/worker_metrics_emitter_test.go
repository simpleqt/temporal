package workers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
)

func TestWorkersPerProcessMetric(t *testing.T) {
	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	emitter := &workerMetricsEmitter{
		handler: captureHandler,
		config:  WorkerMetricsConfig{},
	}

	nsID := namespace.ID("ns-id")
	nsName := namespace.Name("ns-name")

	emitter.emit(nsID, nsName, nil, []*workerpb.WorkerHeartbeat{
		{WorkerInstanceKey: "w1"},
		{WorkerInstanceKey: "w2"},
		{WorkerInstanceKey: "w3"},
	})

	snapshot := capture.Snapshot()
	recordings := snapshot[metrics.WorkerRegistryWorkersPerProcess.Name()]
	require.Len(t, recordings, 1)
	require.Equal(t, int64(3), recordings[0].Value)
}

func TestPollerAutoscalingMetrics(t *testing.T) {
	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	emitter := &workerMetricsEmitter{
		handler: captureHandler,
		config: WorkerMetricsConfig{
			EnablePluginMetrics:            dynamicconfig.GetBoolPropertyFn(false),
			EnablePollerAutoscalingMetrics: dynamicconfig.GetBoolPropertyFn(true),
		},
	}

	testNamespaceID := namespace.ID("test-namespace-id")
	testNamespaceName := namespace.Name("test-namespace")
	testTaskQueue := "test-task-queue"

	// Worker 1: workflow autoscaling enabled, activity disabled
	worker1 := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: "worker_1",
		TaskQueue:         testTaskQueue,
		WorkflowPollerInfo: &workerpb.WorkerPollerInfo{
			IsAutoscaling: true,
		},
		ActivityPollerInfo: &workerpb.WorkerPollerInfo{
			IsAutoscaling: false,
		},
	}

	// Worker 2: workflow, activity, and nexus all enabled
	worker2 := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: "worker_2",
		TaskQueue:         testTaskQueue,
		WorkflowPollerInfo: &workerpb.WorkerPollerInfo{
			IsAutoscaling: true,
		},
		ActivityPollerInfo: &workerpb.WorkerPollerInfo{
			IsAutoscaling: true,
		},
		NexusPollerInfo: &workerpb.WorkerPollerInfo{
			IsAutoscaling: true,
		},
	}

	// Worker 3: workflow only
	worker3 := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: "worker_3",
		TaskQueue:         testTaskQueue,
		WorkflowPollerInfo: &workerpb.WorkerPollerInfo{
			IsAutoscaling: true,
		},
	}

	emitter.emit(testNamespaceID, testNamespaceName, nil, []*workerpb.WorkerHeartbeat{worker1, worker2, worker3})

	snapshot := capture.Snapshot()
	autoscalingMetrics := snapshot[metrics.PollerAutoscalingHeartbeatCount.Name()]

	// Counter increments per heartbeat: workflow x3, activity x1, nexus x1 = 5 total recordings
	assert.Len(t, autoscalingMetrics, 5, "expected 5 counter increments")

	taskTypeCounts := make(map[string]int)
	for _, m := range autoscalingMetrics {
		taskTypeCounts[m.Tags[metrics.TaskTypeTagName]]++
		assert.Equal(t, string(testNamespaceName), m.Tags["namespace"])
		assert.Equal(t, "__omitted__", m.Tags["taskqueue"])
	}
	assert.Equal(t, 3, taskTypeCounts[enumspb.TASK_QUEUE_TYPE_WORKFLOW.String()], "workflow should have 3 increments")
	assert.Equal(t, 1, taskTypeCounts[enumspb.TASK_QUEUE_TYPE_ACTIVITY.String()], "activity should have 1 increment")
	assert.Equal(t, 1, taskTypeCounts[enumspb.TASK_QUEUE_TYPE_NEXUS.String()], "nexus should have 1 increment")
}

func TestPollerAutoscalingMetricsDisabled(t *testing.T) {
	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	emitter := &workerMetricsEmitter{
		handler: captureHandler,
		config: WorkerMetricsConfig{
			EnablePluginMetrics:            dynamicconfig.GetBoolPropertyFn(false),
			EnablePollerAutoscalingMetrics: dynamicconfig.GetBoolPropertyFn(false),
		},
	}

	worker1 := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: "worker_1",
		TaskQueue:         "test-task-queue",
		WorkflowPollerInfo: &workerpb.WorkerPollerInfo{
			IsAutoscaling: true,
		},
	}

	testNamespaceID := namespace.ID("test-namespace-id")
	testNamespaceName := namespace.Name("test-namespace")
	emitter.emit(testNamespaceID, testNamespaceName, nil, []*workerpb.WorkerHeartbeat{worker1})

	snapshot := capture.Snapshot()
	autoscalingMetrics := snapshot[metrics.PollerAutoscalingHeartbeatCount.Name()]
	assert.Empty(t, autoscalingMetrics, "should not record autoscaling metrics when disabled")
}

func TestPerWorkerPollerMetrics(t *testing.T) {
	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	emitter := &workerMetricsEmitter{
		handler: captureHandler,
		config: WorkerMetricsConfig{
			EnablePerWorkerPollerMetrics: dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		},
	}

	testNamespaceID := namespace.ID("test-namespace-id")
	testNamespaceName := namespace.Name("test-namespace")
	testTaskQueue := "test-task-queue"

	worker := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: "worker_1",
		TaskQueue:         testTaskQueue,
		HostInfo: &workerpb.WorkerHostInfo{
			HostName:  "host-a",
			ProcessId: "4242",
		},
		// The SDK always fills all four poller blocks; the slots blocks are what indicate which
		// sub-workers actually exist. This worker runs workflow and activity, not nexus.
		WorkflowTaskSlotsInfo:    &workerpb.WorkerSlotsInfo{SlotSupplierKind: "Fixed"},
		ActivityTaskSlotsInfo:    &workerpb.WorkerSlotsInfo{SlotSupplierKind: "Fixed"},
		WorkflowPollerInfo:       &workerpb.WorkerPollerInfo{IsAutoscaling: true, TargetPollers: 10, CurrentPollers: 8},
		WorkflowStickyPollerInfo: &workerpb.WorkerPollerInfo{IsAutoscaling: true, TargetPollers: 12, CurrentPollers: 12},
		ActivityPollerInfo:       &workerpb.WorkerPollerInfo{IsAutoscaling: true, TargetPollers: 30, CurrentPollers: 23},
		NexusPollerInfo:          &workerpb.WorkerPollerInfo{},
	}

	emitter.emit(testNamespaceID, testNamespaceName, nil, []*workerpb.WorkerHeartbeat{worker})

	snapshot := capture.Snapshot()

	type key struct{ taskType, pollerKind string }
	collect := func(name string) map[key]float64 {
		out := make(map[key]float64)
		for _, m := range snapshot[name] {
			require.Equal(t, string(testNamespaceName), m.Tags["namespace"])
			require.Equal(t, "host-a", m.Tags[metrics.WorkerHostNameTagName])
			require.Equal(t, "4242", m.Tags[metrics.WorkerProcessIDTagName])
			out[key{m.Tags[metrics.TaskTypeTagName], m.Tags[metrics.PollerKindTagName]}] = m.Value.(float64)
		}
		return out
	}

	workflow := enumspb.TASK_QUEUE_TYPE_WORKFLOW.String()
	activity := enumspb.TASK_QUEUE_TYPE_ACTIVITY.String()

	targets := collect(metrics.WorkerPollerTarget.Name())
	require.Len(t, targets, 3, "one series per present poller; nexus is absent")
	require.InDelta(t, float64(10), targets[key{workflow, pollerKindNormal}], 0)
	require.InDelta(t, float64(12), targets[key{workflow, pollerKindSticky}], 0)
	require.InDelta(t, float64(30), targets[key{activity, pollerKindNormal}], 0)

	currents := collect(metrics.WorkerPollerCurrent.Name())
	require.Len(t, currents, 3)
	require.InDelta(t, float64(8), currents[key{workflow, pollerKindNormal}], 0)
	require.InDelta(t, float64(12), currents[key{workflow, pollerKindSticky}], 0)
	require.InDelta(t, float64(23), currents[key{activity, pollerKindNormal}], 0)
}

// A worker with a fixed poller count reports no autoscaler target. The current count is still
// worth seeing while debugging, so the series is emitted with a zero target rather than skipped.
func TestPerWorkerPollerMetricsWithoutAutoscaler(t *testing.T) {
	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	emitter := &workerMetricsEmitter{
		handler: captureHandler,
		config: WorkerMetricsConfig{
			EnablePerWorkerPollerMetrics: dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		},
	}

	worker := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey:     "worker_1",
		TaskQueue:             "test-task-queue",
		HostInfo:              &workerpb.WorkerHostInfo{HostName: "host-a", ProcessId: "4242"},
		ActivityTaskSlotsInfo: &workerpb.WorkerSlotsInfo{SlotSupplierKind: "Fixed"},
		ActivityPollerInfo:    &workerpb.WorkerPollerInfo{IsAutoscaling: false, TargetPollers: 0, CurrentPollers: 5},
	}

	emitter.emit(namespace.ID("test-namespace-id"), namespace.Name("test-namespace"), nil,
		[]*workerpb.WorkerHeartbeat{worker})

	snapshot := capture.Snapshot()
	targets := snapshot[metrics.WorkerPollerTarget.Name()]
	require.Len(t, targets, 1)
	require.InDelta(t, float64(0), targets[0].Value, 0)

	currents := snapshot[metrics.WorkerPollerCurrent.Name()]
	require.Len(t, currents, 1)
	require.InDelta(t, float64(5), currents[0].Value, 0)
}

func TestPerWorkerPollerMetricsMissingHostInfo(t *testing.T) {
	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	emitter := &workerMetricsEmitter{
		handler: captureHandler,
		config: WorkerMetricsConfig{
			EnablePerWorkerPollerMetrics: dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
		},
	}

	worker := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey:     "worker_1",
		TaskQueue:             "test-task-queue",
		ActivityTaskSlotsInfo: &workerpb.WorkerSlotsInfo{SlotSupplierKind: "Fixed"},
		ActivityPollerInfo:    &workerpb.WorkerPollerInfo{IsAutoscaling: true, TargetPollers: 7, CurrentPollers: 7},
	}

	emitter.emit(namespace.ID("test-namespace-id"), namespace.Name("test-namespace"), nil,
		[]*workerpb.WorkerHeartbeat{worker})

	targets := capture.Snapshot()[metrics.WorkerPollerTarget.Name()]
	require.Len(t, targets, 1)
	// metrics.unknownValue, unexported.
	require.Equal(t, "_unknown_", targets[0].Tags[metrics.WorkerHostNameTagName])
	require.Equal(t, "_unknown_", targets[0].Tags[metrics.WorkerProcessIDTagName])
}

func TestPerWorkerPollerMetricsDisabled(t *testing.T) {
	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	// Poller autoscaling metrics on, per-worker metrics off: the two gates are independent.
	emitter := &workerMetricsEmitter{
		handler: captureHandler,
		config: WorkerMetricsConfig{
			EnablePollerAutoscalingMetrics: dynamicconfig.GetBoolPropertyFn(true),
			EnablePerWorkerPollerMetrics:   dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		},
	}

	worker := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey:     "worker_1",
		TaskQueue:             "test-task-queue",
		HostInfo:              &workerpb.WorkerHostInfo{HostName: "host-a", ProcessId: "4242"},
		ActivityTaskSlotsInfo: &workerpb.WorkerSlotsInfo{SlotSupplierKind: "Fixed"},
		ActivityPollerInfo:    &workerpb.WorkerPollerInfo{IsAutoscaling: true, TargetPollers: 8, CurrentPollers: 8},
	}

	emitter.emit(namespace.ID("test-namespace-id"), namespace.Name("test-namespace"), nil,
		[]*workerpb.WorkerHeartbeat{worker})

	snapshot := capture.Snapshot()
	require.Empty(t, snapshot[metrics.WorkerPollerTarget.Name()])
	require.Empty(t, snapshot[metrics.WorkerPollerCurrent.Name()])
	require.Len(t, snapshot[metrics.PollerAutoscalingHeartbeatCount.Name()], 1,
		"the autoscaling adoption counter is gated independently")
}

// Temporal's own internal workers heartbeat too. They are excluded so the per-worker metric
// shows only the user's workers, matching ListWorkers' default behavior.
func TestPerWorkerPollerMetricsExcludesSystemWorkers(t *testing.T) {
	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	emitter := &workerMetricsEmitter{
		handler: captureHandler,
		config: WorkerMetricsConfig{
			EnablePerWorkerPollerMetrics: dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true),
			// Enabled so the assertion can name the task queue rather than "__omitted__".
			BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true),
		},
	}

	newWorker := func(key, taskQueue string) *workerpb.WorkerHeartbeat {
		return &workerpb.WorkerHeartbeat{
			WorkerInstanceKey:     key,
			TaskQueue:             taskQueue,
			HostInfo:              &workerpb.WorkerHostInfo{HostName: "host-a", ProcessId: "4242"},
			ActivityTaskSlotsInfo: &workerpb.WorkerSlotsInfo{SlotSupplierKind: "Fixed"},
			ActivityPollerInfo:    &workerpb.WorkerPollerInfo{IsAutoscaling: true, TargetPollers: 3, CurrentPollers: 3},
		}
	}

	emitter.emit(namespace.ID("test-namespace-id"), namespace.Name("test-namespace"), nil,
		[]*workerpb.WorkerHeartbeat{
			newWorker("user_worker", "user-task-queue"),
			// primitives.internalTaskQueuePrefix, unexported.
			newWorker("system_worker", "temporal-sys-scanner-tq"),
		})

	targets := capture.Snapshot()[metrics.WorkerPollerTarget.Name()]
	require.Len(t, targets, 1, "only the user worker should be reported")
	require.Equal(t, "user-task-queue", targets[0].Tags["taskqueue"])
}
