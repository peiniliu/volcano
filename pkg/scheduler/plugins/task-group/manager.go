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
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog"

	"volcano.sh/volcano/pkg/scheduler/api"
)

// JobManager is struct used to save infos about taskgroups of a job
type JobManager struct {
	jobID api.JobID

	groups      []*Group                          //groups
	podInGroup  map[types.UID]int                 //pod->groupid
	podInTask   map[types.UID]string              //pod->taskname
	taskOverPod map[string]map[types.UID]struct{} //[taskname]->podstru

	taskGroups     map[string]int //[taskname]->[nGroups]
	taskExistOrder map[string]int //[taskname]->[order]

	groupMaxSize int
	nodeGroupSet map[string]map[int]int //[nodeName]->[groupindex]->numbertasks
}

// NewJobManager creates a new job manager for job
func NewJobManager(jobID api.JobID) *JobManager {
	return &JobManager{
		jobID: jobID,

		groups:      make([]*Group, 0),
		podInGroup:  make(map[types.UID]int),
		podInTask:   make(map[types.UID]string),
		taskOverPod: make(map[string]map[types.UID]struct{}),

		taskGroups:     make(map[string]int),
		taskExistOrder: make(map[string]int),

		groupMaxSize: 0,
		nodeGroupSet: make(map[string]map[int]int),
	}
}

//read annotation to manager
func (jm *JobManager) ApplyTaskGroup(tg *TaskGroup) {
	//taskgroups
	jm.taskGroups = tg.Groups
	//taskorder
	length := len(tg.TaskOrder)
	for i, taskName := range tg.TaskOrder {
		jm.taskExistOrder[taskName] = length - i
	}
	// klog.Infof("applyTaskGroup: taskgroups %d, taskexistorder %d", jm.taskGroups["mpiworker"], jm.taskExistOrder["mpiworker"])
}

// ConstructGroup builds group for tasks
func (jm *JobManager) ConstructGroup(tasks map[api.TaskID]*api.TaskInfo) {
	taskWithoutGroup := jm.buildTaskInfo(tasks)

	// klog.Infof("construct groups: %d", len(taskWithoutGroup))
	// for _, task := range tasks {
	// 	pod := task.Pod
	// 	klog.Infof("podingroup: group=%d ---- %s, %s, %d", len(jm.groups),
	// 		getTaskName(task), pod.Name, jm.podInGroup[pod.UID])
	// }

	o := TaskOrder{
		tasks:   taskWithoutGroup,
		manager: jm,
	}

	sort.Sort(sort.Reverse(&o))
	// klog.Infof("reversed tasks: %d", len(o.tasks))
	// for _, task := range o.tasks {
	// 	pod := task.Pod
	// 	klog.Infof("podingroup: group=%d ---- %s, %s, %d", len(jm.groups),
	// 		getTaskName(task), pod.Name, jm.podInGroup[pod.UID])
	// }

	jm.buildGroup(o.tasks)

}

//set flags to task
func (jm *JobManager) buildTaskInfo(tasks map[api.TaskID]*api.TaskInfo) []*api.TaskInfo {
	taskWithoutGroup := make([]*api.TaskInfo, 0, len(tasks))
	for _, task := range tasks {
		pod := task.Pod

		taskName := getTaskName(task)
		if taskName == "" {
			jm.MarkOutOfGroup(pod.UID)
			continue
		}
		if _, hasGroup := jm.taskGroups[taskName]; !hasGroup {
			jm.MarkOutOfGroup(pod.UID)
			continue
		}

		jm.podInTask[pod.UID] = taskName
		taskSet, ok := jm.taskOverPod[taskName]
		if !ok {
			taskSet = make(map[types.UID]struct{})
			jm.taskOverPod[taskName] = taskSet
		}
		taskSet[pod.UID] = struct{}{}
		taskWithoutGroup = append(taskWithoutGroup, task)
	}
	return taskWithoutGroup
}

// MarkOutOfgroup indicates task is outside of any group
func (jm *JobManager) MarkOutOfGroup(uid types.UID) {
	jm.podInGroup[uid] = OutOfBucket
}

//allocate task in group
func (jm *JobManager) buildGroup(taskWithOrder []*api.TaskInfo) {
	// klog.Infof("buildgroup: %d", len(taskWithOrder))

	nodeGroupMapping := make(map[string]*Group) // nodename->group

	for _, task := range taskWithOrder {
		// klog.Infof("jobID %s task with order task %s/%s",
		// 	jm.jobID, task.Namespace, task.Name)

		taskName := getTaskName(task)
		nGroups := jm.taskGroups[taskName]
		if len(jm.groups) < nGroups {
			for i := 0; i < nGroups; i++ {
				jm.NewGroup(i)
			}
		}
	}

	for _, task := range taskWithOrder {

		var selectedGroup *Group
		taskName := getTaskName(task)
		if task.NodeName != "" {
			selectedGroup = nodeGroupMapping[task.NodeName]
		} else {
			// choose balance resource between groups
			sort.Sort(SortByScore(jm.groups))
			// for i, group := range jm.groups {
			// 	klog.Infof("groupid %d, groupindex %d , groupscore %d, taskset %d",
			// 		i, group.index, group.reqScore, group.taskNameSet["mpiworker"])
			// }
			selectedGroup = jm.groups[0]
		}

		jm.AddTaskToGroup(selectedGroup, taskName, task)

		pod := task.Pod
		klog.Infof("buildpodingroup: %s, %s, %d",
			getTaskName(task), pod.Name, jm.podInGroup[pod.UID])
	}
}

// AddTaskToGroup adds task into group
func (jm *JobManager) AddTaskToGroup(group *Group, taskName string, task *api.TaskInfo) {
	jm.podInGroup[task.Pod.UID] = group.index
	group.AddTask(taskName, task)
	if size := len(group.tasks) + group.boundTask; size > jm.groupMaxSize {
		jm.groupMaxSize = size
	}
}

// creates a new group
func (jm *JobManager) NewGroup(index int) *Group {
	group := NewGroup(index)
	jm.groups = append(jm.groups, group)
	return group
}

// binds task
func (jm *JobManager) BindTask(task *api.TaskInfo) {

	if taskName := getTaskName(task); taskName != "" {
		group := jm.GetGroupByTask(task)
		if group != nil {
			group.TaskBound(task)

			set, ok := jm.nodeGroupSet[task.NodeName]
			if !ok {
				set = make(map[int]int)
				jm.nodeGroupSet[task.NodeName] = set
			}
			set[group.index]++
		}
	}

}

// L compared with R, -1 for L < R, 0 for L == R, 1 for L > R
func (jm *JobManager) taskGroupOrder(L, R *api.TaskInfo) int {
	LTaskName := jm.podInTask[L.Pod.UID]
	RTaskName := jm.podInTask[R.Pod.UID]

	// in the same vk task, they are equal
	if LTaskName == RTaskName {
		return 0
	}

	// use user defined order firstly
	LOrder := jm.taskExistOrder[LTaskName]
	ROrder := jm.taskExistOrder[RTaskName]
	if LOrder != ROrder {
		if LOrder > ROrder {
			return 1
		}
		return -1
	}
	return 0
}

// get group by task
func (jm *JobManager) GetGroupByTask(task *api.TaskInfo) *Group {
	index, ok := jm.podInGroup[task.Pod.UID]
	if !ok || index == OutOfBucket {
		return nil
	}
	sort.Sort(SortByIndex(jm.groups))
	group := jm.groups[index]
	return group
}

func (jm *JobManager) String() string {

	msg := []string{
		fmt.Sprintf("%s - job %s max %d ",
			PluginName, jm.jobID, jm.groupMaxSize,
		),
	}

	for _, group := range jm.groups {
		groupMsg := fmt.Sprintf("b:%d -- ", group.index)
		var info []string
		for _, task := range group.tasks {
			info = append(info, task.Pod.Name)
		}
		groupMsg += strings.Join(info, ", ")
		groupMsg += "|"

		info = nil
		for nodeName, count := range group.node {
			info = append(info, fmt.Sprintf("n%s-%d", nodeName, count))
		}
		groupMsg += strings.Join(info, ", ")

		msg = append(msg, "["+groupMsg+"]")
	}
	return strings.Join(msg, " ")
}
