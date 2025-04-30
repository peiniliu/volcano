package energy

import (
	"fmt"
	"time"

	"volcano.sh/volcano/pkg/scheduler/metrics/source"

	"k8s.io/klog/v2"
	k8sFramework "k8s.io/kubernetes/pkg/scheduler/framework"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const (
	// PluginName indicates name of volcano scheduler plugin.
	PluginName            = "energy"
	thresholdSection      = "thresholds"
	MetricsActiveTime     = 5 * time.Minute
	NodeUsageCPUExtend    = "the CPU load of the node exceeds the upper limit."
	NodeUsageMemoryExtend = "the memory load of the node exceeds the upper limit."
)

/*
   actions: "enqueue, allocate, backfill"
   tiers:
   - plugins:
     - name: energy
       enablePredicate: false  # If the value is false, new pod scheduling is not disabled when the node load reaches the threshold. If the value is true or left blank, new pod scheduling is disabled.
       arguments:
         energy.weight: 5
         cpu.weight: 1
         memory.weight: 1
         thresholds:
           cpu: 80
           mem: 80
*/

const AVG string = "average"

type energyPlugin struct {
	pluginArguments framework.Arguments
	energyWeight    int
	cpuWeight       int
	memoryWeight    int
	usageType       string
	cpuThresholds   float64
	memThresholds   float64
	period          string
}

// New function returns energyPlugin object
func New(args framework.Arguments) framework.Plugin {
	var plugin = &energyPlugin{
		pluginArguments: args,
		energyWeight:    5,
		cpuWeight:       1,
		memoryWeight:    1,
		usageType:       AVG,
		cpuThresholds:   90,
		memThresholds:   90,
		period:          source.NODE_METRICS_PERIOD,
	}
	args.GetInt(&plugin.energyWeight, "energy.weight")
	args.GetInt(&plugin.cpuWeight, "cpu.weight")
	args.GetInt(&plugin.memoryWeight, "memory.weight")

	argsValue, ok := plugin.pluginArguments[thresholdSection]
	if !ok {
		klog.Errorf("Failed to obtain thresholds information, energy plugin arguments is %v", plugin.pluginArguments)
		return plugin
	}

	thresholdArgs, ok := argsValue.(map[interface{}]interface{})
	if !ok {
		klog.Errorf("Failed to convert the thresholds information, thresholds args values is %v", argsValue)
		return plugin
	}
	for resourceName, threshold := range thresholdArgs {
		resource, _ := resourceName.(string)
		value, _ := threshold.(int)
		switch resource {
		case "cpu":
			plugin.cpuThresholds = float64(value)
		case "mem":
			plugin.memThresholds = float64(value)
		}
	}

	return plugin
}

func (up *energyPlugin) Name() string {
	return PluginName
}

func (up *energyPlugin) OnSessionOpen(ssn *framework.Session) {
	klog.V(5).Infof("Enter energy plugin ...")
	defer func() {
		klog.V(5).Infof("Leaving energy plugin ...")
	}()

	if klog.V(4).Enabled() {
		for node, nodeInfo := range ssn.Nodes {
			klog.V(4).Infof("node:%v, cpu usage:%v, mem usage:%v, power consumption: %v, metrics time is %v",
				node, nodeInfo.ResourceUsage.CPUUsageAvg, nodeInfo.ResourceUsage.MEMUsageAvg, 
				nodeInfo.ResourceUsage.PowerUsageAvg, nodeInfo.ResourceUsage.MetricsTime)
		}
	}

	predicateFn := func(task *api.TaskInfo, node *api.NodeInfo) ([]*api.Status, error) {
		predicateStatus := make([]*api.Status, 0)
		usageStatus := &api.Status{}

		now := time.Now()
		if up.period == "" || now.Sub(node.ResourceUsage.MetricsTime) > MetricsActiveTime {
			klog.V(4).Infof("The period(%s) is empty or the metrics data is not updated for more than %v minutes, "+
				"Energy plugin filter for task %s/%s on node %s pass, metrics time is %v. ", up.period, MetricsActiveTime, task.Namespace, task.Name, node.Name, node.ResourceUsage.MetricsTime)

			usageStatus.Code = api.Success
			predicateStatus = append(predicateStatus, usageStatus)
			return predicateStatus, nil
		}

		klog.V(4).Infof("predicateFn cpuUsageAvg:%v,predicateFn memUsageAvg:%v", up.cpuThresholds, up.memThresholds)
		if node.ResourceUsage.CPUUsageAvg[up.period] > up.cpuThresholds {
			klog.V(3).Infof("Node %s cpu usage %f exceeds the threshold %f", node.Name, node.ResourceUsage.CPUUsageAvg[up.period], up.cpuThresholds)
			usageStatus.Code = api.UnschedulableAndUnresolvable
			usageStatus.Reason = NodeUsageCPUExtend
			predicateStatus = append(predicateStatus, usageStatus)
			return predicateStatus, fmt.Errorf("Plugin %s predicates failed, because of %s", up.Name(), NodeUsageCPUExtend)
		}
		if node.ResourceUsage.MEMUsageAvg[up.period] > up.memThresholds {
			klog.V(3).Infof("Node %s mem usage %f exceeds the threshold %f", node.Name, node.ResourceUsage.MEMUsageAvg[up.period], up.memThresholds)
			usageStatus.Code = api.UnschedulableAndUnresolvable
			usageStatus.Reason = NodeUsageMemoryExtend
			predicateStatus = append(predicateStatus, usageStatus)
			return predicateStatus, fmt.Errorf("Plugin %s predicates failed, because of %s", up.Name(), NodeUsageMemoryExtend)
		}

		klog.V(4).Infof("Energy plugin filter for task %s/%s on node %s pass.", task.Namespace, task.Name, node.Name)
		return predicateStatus, nil
	}

	nodeOrderFn := func(task *api.TaskInfo, node *api.NodeInfo) (float64, error) {
		score := 0.0
		now := time.Now()
		if up.period == "" || now.Sub(node.ResourceUsage.MetricsTime) > MetricsActiveTime {
			klog.V(4).Infof("The period(%s) is empty or the metrics data is not updated for more than %v minutes, "+
				"Energy plugin score for task %s/%s on node %s is 0, metrics time is %v. ", up.period, MetricsActiveTime, task.Namespace, task.Name, node.Name, node.ResourceUsage.MetricsTime)
			return 0, nil
		}

		powerUsage, exist := node.ResourceUsage.PowerUsageAvg[up.period]
		klog.V(4).Infof("Node %s power consumption is %f.", node.Name, powerUsage)
		if !exist {
			return 0, nil
		}
		powerScore := (100 - powerUsage) / 100 * float64(up.cpuWeight)

		//cost function
		// score = powerScore * energy price??
		score = powerScore
		
		score *= float64(k8sFramework.MaxNodeScore * int64(up.energyWeight))

		klog.V(4).Infof("Node %s score for task %s is %f.", node.Name, task.Name, score)
		return score, nil
	}

	ssn.AddPredicateFn(up.Name(), predicateFn)
	ssn.AddNodeOrderFn(up.Name(), nodeOrderFn)
}

func (up *usagePlugin) OnSessionClose(ssn *framework.Session) {}
