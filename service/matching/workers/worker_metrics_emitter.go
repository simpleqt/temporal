package workers

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tqid"
)

// Values for the poller_kind tag on the per-worker poller metrics.
const (
	pollerKindNormal = "normal"
	pollerKindSticky = "sticky"
)

// WorkerMetricsConfig contains dynamic config flags for worker-related metrics.
type WorkerMetricsConfig struct {
	EnablePluginMetrics            dynamicconfig.BoolPropertyFn
	EnablePollerAutoscalingMetrics dynamicconfig.BoolPropertyFn
	EnablePerWorkerPollerMetrics   dynamicconfig.BoolPropertyFnWithNamespaceFilter
	BreakdownMetricsByTaskQueue    dynamicconfig.BoolPropertyFnWithTaskQueueFilter
	ExternalPayloadsEnabled        dynamicconfig.BoolPropertyFnWithNamespaceFilter
}

// workerMetricsEmitter encapsulates logic for emitting metrics derived from worker heartbeats.
type workerMetricsEmitter struct {
	handler metrics.Handler
	config  WorkerMetricsConfig
}

// metricGates holds the resolved dynamic config gates for one emit call, so the nil-guard
// and namespace lookup happen once per batch rather than once per worker.
type metricGates struct {
	pluginMetrics            bool
	pollerAutoscalingMetrics bool
	perWorkerPollerMetrics   bool
	storageDriverMetrics     bool
}

func (e *workerMetricsEmitter) resolveGates(nsName namespace.Name) metricGates {
	return metricGates{
		pluginMetrics:            e.config.EnablePluginMetrics != nil && e.config.EnablePluginMetrics(),
		pollerAutoscalingMetrics: e.config.EnablePollerAutoscalingMetrics != nil && e.config.EnablePollerAutoscalingMetrics(),
		perWorkerPollerMetrics:   e.config.EnablePerWorkerPollerMetrics != nil && e.config.EnablePerWorkerPollerMetrics(nsName.String()),
		storageDriverMetrics:     e.config.ExternalPayloadsEnabled != nil && e.config.ExternalPayloadsEnabled(nsName.String()),
	}
}

func (e *workerMetricsEmitter) emit(nsID namespace.ID, nsName namespace.Name, principal *commonpb.Principal, heartbeats []*workerpb.WorkerHeartbeat) {
	// The SDK aggregates all workers on the same Client into one heartbeat RPC, so
	// len(heartbeats) approximates workers-per-process. It's per-Client-per-Namespace,
	// not strictly per-process, but multiple Clients per process is uncommon in practice.
	metrics.WorkerRegistryWorkersPerProcess.With(e.handler).Record(int64(len(heartbeats)))

	gates := e.resolveGates(nsName)

	recordedPlugins := make(map[string]bool)
	recordedDrivers := make(map[string]bool)

	for _, hb := range heartbeats {
		// Activity slots metric (always enabled)
		if hb.ActivityTaskSlotsInfo != nil {
			metrics.WorkerRegistryActivitySlotsUsed.With(e.handler).Record(int64(hb.ActivityTaskSlotsInfo.CurrentUsedSlots))
		}

		// Plugin metrics (if enabled)
		if gates.pluginMetrics {
			for _, pluginInfo := range hb.Plugins {
				pluginName := pluginInfo.Name
				if !recordedPlugins[pluginName] {
					metrics.WorkerPluginNameMetric.
						With(e.handler).
						Record(1, metrics.NamespaceTag(nsName.String()), metrics.WorkerPluginNameTag(pluginName))
					recordedPlugins[pluginName] = true
				}
			}
		}

		// Poller autoscaling metrics (if enabled)
		if gates.pollerAutoscalingMetrics {
			e.emitPollerAutoscaling(nsID, nsName, hb)
		}

		// Per-worker poller metrics (if enabled for this namespace). System workers are
		// excluded to match ListWorkers, which hides them by default.
		if gates.perWorkerPollerMetrics && !isSystemWorker(principal, hb.GetTaskQueue()) {
			e.emitPerWorkerPollerMetrics(nsID, nsName, hb)
		}

		// Storage driver metrics (if external payloads enabled)
		if gates.storageDriverMetrics {
			for _, driver := range hb.GetDrivers() {
				driverType := driver.GetType()
				if !recordedDrivers[driverType] {
					metrics.WorkerStorageDriverTypeMetric.
						With(e.handler).
						Record(1, metrics.NamespaceTag(nsName.String()), metrics.WorkerStorageDriverTypeTag(driverType))
					recordedDrivers[driverType] = true
				}
			}
		}
	}
}

// taskQueueScope returns a handler scoped to the worker's task queue and the given task type,
// honoring the per-task-queue cardinality breakdown setting.
func (e *workerMetricsEmitter) taskQueueScope(
	family *tqid.TaskQueueFamily,
	nsName namespace.Name,
	taskQueueName string,
	taskType enumspb.TaskQueueType,
	tags ...metrics.Tag,
) metrics.Handler {
	tq := family.TaskQueue(taskType)
	breakdownByTQ := e.config.BreakdownMetricsByTaskQueue != nil &&
		e.config.BreakdownMetricsByTaskQueue(nsName.String(), taskQueueName, taskType)
	return metrics.GetPerTaskQueueScope(e.handler, nsName.String(), tq, breakdownByTQ, tags...)
}

func (e *workerMetricsEmitter) emitPollerAutoscaling(nsID namespace.ID, nsName namespace.Name, hb *workerpb.WorkerHeartbeat) {
	family, err := tqid.NewTaskQueueFamily(nsID.String(), hb.GetTaskQueue())
	if err != nil {
		return
	}

	recordAutoscaling := func(taskType enumspb.TaskQueueType) {
		handler := e.taskQueueScope(family, nsName, hb.GetTaskQueue(), taskType)
		metrics.PollerAutoscalingHeartbeatCount.With(handler).Record(1)
	}

	if hb.WorkflowPollerInfo.GetIsAutoscaling() {
		recordAutoscaling(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	}
	if hb.ActivityPollerInfo.GetIsAutoscaling() {
		recordAutoscaling(enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	}
	if hb.NexusPollerInfo.GetIsAutoscaling() {
		recordAutoscaling(enumspb.TASK_QUEUE_TYPE_NEXUS)
	}
}

// emitPerWorkerPollerMetrics emits one target/current pair per poller for a single worker,
// identified by host name and process id. The worker instance key is deliberately not used as
// a tag: it is a UUID regenerated on every worker restart, and emitted series are never
// reclaimed. Host name and process id are bounded per host and survive a worker restart.
func (e *workerMetricsEmitter) emitPerWorkerPollerMetrics(nsID namespace.ID, nsName namespace.Name, hb *workerpb.WorkerHeartbeat) {
	family, err := tqid.NewTaskQueueFamily(nsID.String(), hb.GetTaskQueue())
	if err != nil {
		return
	}

	hostInfo := hb.GetHostInfo()
	workerTags := []metrics.Tag{
		metrics.WorkerHostNameTag(hostInfo.GetHostName()),
		metrics.WorkerProcessIDTag(hostInfo.GetProcessId()),
	}

	// Sticky workflow pollers have no task queue type of their own, so the poller kind is what
	// separates them from normal workflow pollers.
	//
	// The SDK populates all four poller blocks unconditionally, so a present block does not mean
	// the worker runs that poller. Presence is taken from the matching slots block, which the SDK
	// only fills in when the corresponding sub-worker exists. Without this, every worker emits
	// zero-valued series for the poller types it does not run.
	recordPoller := func(taskType enumspb.TaskQueueType, info *workerpb.WorkerPollerInfo, kind string, present bool) {
		if !present {
			return
		}
		tags := append([]metrics.Tag{metrics.PollerKindTag(kind)}, workerTags...)
		handler := e.taskQueueScope(family, nsName, hb.GetTaskQueue(), taskType, tags...)
		metrics.WorkerPollerTarget.With(handler).Record(float64(info.GetTargetPollers()))
		metrics.WorkerPollerCurrent.With(handler).Record(float64(info.GetCurrentPollers()))
	}

	hasWorkflowWorker := hb.GetWorkflowTaskSlotsInfo() != nil
	recordPoller(enumspb.TASK_QUEUE_TYPE_WORKFLOW, hb.GetWorkflowPollerInfo(), pollerKindNormal,
		hasWorkflowWorker)
	// A sticky poller exists only when the workflow poller is auto-scaled and the sticky cache is
	// enabled, in which case its autoscaler seeds a non-zero target.
	recordPoller(enumspb.TASK_QUEUE_TYPE_WORKFLOW, hb.GetWorkflowStickyPollerInfo(), pollerKindSticky,
		hasWorkflowWorker && hb.GetWorkflowStickyPollerInfo().GetTargetPollers() > 0)
	recordPoller(enumspb.TASK_QUEUE_TYPE_ACTIVITY, hb.GetActivityPollerInfo(), pollerKindNormal,
		hb.GetActivityTaskSlotsInfo() != nil)
	recordPoller(enumspb.TASK_QUEUE_TYPE_NEXUS, hb.GetNexusPollerInfo(), pollerKindNormal,
		hb.GetNexusTaskSlotsInfo() != nil)
}
