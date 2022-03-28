/*
Copyright 2021 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package taskgroup

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	k8sFramework "k8s.io/kubernetes/pkg/scheduler/framework"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

type taskGroupPlugin struct {
	arguments framework.Arguments

	weight   int
	managers map[api.JobID]*JobManager
}

// New function returns taskGroupPlugin object
func New(arguments framework.Arguments) framework.Plugin {
	return &taskGroupPlugin{
		arguments: arguments,

		weight:   calculateWeight(arguments),
		managers: make(map[api.JobID]*JobManager),
	}
}

func (p *taskGroupPlugin) Name() string {
	return PluginName
}

// TaskOrderFn returns -1 to make l prior to r.
//
// for example:
// A:
//  | bucket1   | bucket2   | out of bucket
//  | a1 a3     | a2        | a4
// B:
//  | bucket1   | out of bucket
//  | b1 b2     | b3
// the right task order should be:
//   a1 a3 a2 b1 b2 a4 b3
func (p *taskGroupPlugin) TaskOrderFn(l interface{}, r interface{}) int {

	lv, ok := l.(*api.TaskInfo)
	if !ok {
		klog.Errorf("Object is not a taskinfo")
	}
	rv, ok := r.(*api.TaskInfo)
	if !ok {
		klog.Errorf("Object is not a taskinfo")
	}

	// klog.V(3).Infof("taskorder function in taskgroup: jobid-%s", lv.Job)

	lvJobManager := p.managers[lv.Job]
	rvJobManager := p.managers[rv.Job]

	var lvBucket, rvBucket *Group
	if lvJobManager != nil {
		lvBucket = lvJobManager.GetGroupByTask(lv)
	} else {
		klog.V(4).Infof("No job manager for job <ID: %s>, do not return task order.", lv.Job)
		return 0
	}
	if rvJobManager != nil {
		rvBucket = rvJobManager.GetGroupByTask(rv)
	} else {
		klog.V(4).Infof("No job manager for job <ID: %s>, do not return task order.", rv.Job)
		return 0
	}

	// the one have bucket would always prior to another
	lvInBucket := lvBucket != nil
	rvInBucket := rvBucket != nil
	if lvInBucket != rvInBucket {
		if lvInBucket {
			return -1
		}
		return 1
	}

	// comparison between job is not the duty of this plugin
	if lv.Job != rv.Job {
		return 0
	}

	// task out of bucket have no order
	if !lvInBucket && !rvInBucket {
		return 0
	}

	// the big bucket should prior to small one
	lvHasTask := len(lvBucket.tasks)
	rvHasTask := len(rvBucket.tasks)
	if lvHasTask != rvHasTask {
		if lvHasTask > rvHasTask {
			return -1
		}
		return 1
	}

	lvBucketIndex := lvBucket.index
	rvBucketIndex := rvBucket.index
	// in the same bucket, the taskOrder is ok
	if lvBucketIndex == rvBucketIndex {
		taskOrder := lvJobManager.taskGroupOrder(lv, rv)
		return -taskOrder
	}

	// the old bucket should prior to young one
	if lvBucketIndex < rvBucketIndex {
		return -1
	}
	return 1
}

func (p *taskGroupPlugin) calcGroupScore(task *api.TaskInfo, node *api.NodeInfo) (int, *JobManager, error) {
	// task could never fits the node
	maxResource := node.Idle.Clone().Add(node.Releasing)
	if req := task.Resreq; req != nil && maxResource.LessPartly(req, api.Zero) {
		return 0, nil, nil
	}

	jobManager, hasManager := p.managers[task.Job]
	if !hasManager {
		return 0, nil, nil
	}

	group := jobManager.GetGroupByTask(task)
	// task out of bucket
	if group == nil {
		return 0, jobManager, nil
	}

	// 1. bound task in group is the base score of this node
	score := group.node[node.Name]
	// klog.Infof("boundtask score: task %s/%s, node %s, score %d",
	// 	task.Namespace, task.Name, node.Name, score)

	// 2. other groups in the node should be calculated
	if nodeGroupSet := jobManager.nodeGroupSet[node.Name]; nodeGroupSet != nil {
		otherGroupScore := jobManager.checkNodeGroup(group.index, nodeGroupSet)
		if otherGroupScore < 0 {
			score += otherGroupScore
		}
		// klog.Infof("othergroup score: task %s/%s, node %s, score %d",
		// 	task.Namespace, task.Name, node.Name, otherGroupScore)
	}

	// 3. the other tasks in group take into considering
	score += len(group.tasks)
	// klog.Infof("othertask: task %s/%s, node %s, score %d",
	// 	task.Namespace, task.Name, node.Name, score)
	if group.request == nil || group.request.LessEqual(maxResource, api.Zero) {
		return score, jobManager, nil
	}

	remains := group.request.Clone()
	// randomly (by map) take out task to make the group fits the node
	for bucketTaskID, bucketTask := range group.tasks {
		// current task should kept in bucket
		if bucketTaskID == task.Pod.UID || bucketTask.Resreq == nil {
			continue
		}
		remains.Sub(bucketTask.Resreq)
		score--
		if remains.LessEqual(maxResource, api.Zero) {
			break
		}
	}
	// klog.Infof("othertaskremain: task %s/%s, node %s, score %d",
	// 	task.Namespace, task.Name, node.Name, score)

	// klog.Infof("calculate group score: task %s/%s, node %s, score %d",
	// 	task.Namespace, task.Name, node.Name, score)
	// here, the bucket remained request will always fit the maxResource
	return score, jobManager, nil
}

func (jm *JobManager) checkNodeGroup(currentGroupId int, nodeGroupSet map[int]int) int {
	podSameGroup := 0

	for nodeGroupIndex, _ := range nodeGroupSet {
		theSameGroup := nodeGroupIndex == currentGroupId
		if !theSameGroup {
			podSameGroup--
		}
	}

	return podSameGroup
}

func (p *taskGroupPlugin) NodeOrderFn(task *api.TaskInfo, node *api.NodeInfo) (float64, error) {

	score, jobManager, err := p.calcGroupScore(task, node)

	// klog.V(3).Infof("nodeorder function in taskgroup: task-%s, node-%s, score-%d",
	// 	task.Name, node.Name, score)

	if err != nil {
		return 0, err
	}
	fScore := float64(score * p.weight)
	if jobManager != nil && jobManager.groupMaxSize != 0 {
		fScore = fScore * float64(k8sFramework.MaxNodeScore) / float64(jobManager.groupMaxSize)
	}
	klog.V(3).Infof("task %s/%s at node %s has group score %d, score %f",
		task.Namespace, task.Name, node.Name, score, fScore)
	return fScore, nil
}

func (p *taskGroupPlugin) PredicateFn(task *api.TaskInfo, node *api.NodeInfo) error {
	if v1qos.GetPodQOS(task.Pod) != v1.PodQOSGuaranteed {
		klog.V(3).Infof("task %s isn't Guaranteed pod", task.Name)
		return nil
	}

	jobManager, hasManager := p.managers[task.Job]
	if !hasManager {
		return fmt.Errorf("plugin %s predicates failed for task %s/job %s to find jobmanager",
			p.Name(), task.Name, task.Job)
	}

	if len(jobManager.nodeGroupSet) == jobManager.taskGroups[task.Name] {
		//enough group
		klog.V(3).Infof("enough current used number of nodes %s", len(jobManager.nodeGroupSet))
		_, ok := jobManager.nodeGroupSet[node.Name]
		if ok {
			return nil
		} else {
			return fmt.Errorf("plugin %s predicates node %s for task %s/job %s failed due to group restriction",
				p.Name(), node.Name, task.Name, task.Job)
		}
	} else {
		return nil
	}

}

func (p *taskGroupPlugin) AllocateFunc(event *framework.Event) {

	// klog.V(3).Infof("allocate function in taskgroup: jobid-%s, task-%s",
	// 	event.Task.Job, event.Task)
	task := event.Task

	jobManager, hasManager := p.managers[task.Job]
	if !hasManager {
		return
	}
	jobManager.BindTask(task)
}

func readTaskGroupFromPgAnnotations(job *api.JobInfo) (*TaskGroup, error) {
	taskGroupAnno, annotationExist := job.PodGroup.Annotations[taskGroupAnnotations]
	//klog.Info("annotation：%s, %s", taskGroupAnno, annotationExist)

	if !(annotationExist) {
		return nil, nil
	}

	var jobTaskGroup = TaskGroup{
		TaskOrder: nil,
		Groups:    make(map[string]int),
	}

	if annotationExist {
		taskGroupStr := strings.Split(taskGroupAnno, ";") //"worker:4"
		if len(taskGroupStr) == 0 {
			return nil, nil
		}
		jobTaskGroup.TaskOrder = make([]string, len(taskGroupStr))

		for i, str := range taskGroupStr {
			taskAndGroup := strings.Split(str, ":")
			task := taskAndGroup[0]
			group, err := strconv.Atoi(taskAndGroup[1])
			if err != nil {
				klog.V(4).Infof("TaskGroup number error: %s.", err.Error())
				return nil, nil
			}
			jobTaskGroup.TaskOrder[i] = task
			jobTaskGroup.Groups[task] = group
		}
	}

	return &jobTaskGroup, nil
}

func (p *taskGroupPlugin) initBucket(ssn *framework.Session) {

	for jobID, job := range ssn.Jobs {

		if noPendingTasks(job) {
			klog.V(4).Infof("No pending tasks in job <%s/%s> by plugin %s.",
				job.Namespace, job.Name, PluginName)
			continue
		}

		jobTaskGroup, err := readTaskGroupFromPgAnnotations(job)
		if err != nil {
			klog.V(4).Infof("Failed to read task group from job <%s/%s> annotations, error: %s.",
				job.Namespace, job.Name, err.Error())
			continue
		}
		if jobTaskGroup == nil {
			continue
		}

		manager := NewJobManager(jobID)
		manager.ApplyTaskGroup(jobTaskGroup)
		manager.ConstructGroup(job.Tasks)

		p.managers[job.UID] = manager
	}
}

func (p *taskGroupPlugin) OnSessionOpen(ssn *framework.Session) {
	start := time.Now()
	klog.V(3).Infof("start to init task group plugin, weight[%d]", p.weight)

	p.initBucket(ssn)

	ssn.AddPredicateFn(p.Name(), p.PredicateFn)

	ssn.AddTaskOrderFn(p.Name(), p.TaskOrderFn)

	ssn.AddNodeOrderFn(p.Name(), p.NodeOrderFn)

	ssn.AddEventHandler(&framework.EventHandler{
		AllocateFunc: p.AllocateFunc,
	})

	klog.V(3).Infof("finished to init task group plugin, using time %v", time.Since(start))
}

func (p *taskGroupPlugin) OnSessionClose(ssn *framework.Session) {
	p.managers = nil
}
