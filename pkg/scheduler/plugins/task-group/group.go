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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog"

	"volcano.sh/volcano/pkg/scheduler/api"
)

type reqAction int

const (
	reqSub reqAction = iota
	reqAdd
)

// Group is struct used to group tasks
type Group struct {
	index       int
	tasks       map[types.UID]*api.TaskInfo
	taskNameSet map[string]int

	// reqScore is score of resource
	// now, we regard only CPU .
	reqScore float64
	request  *api.Resource

	boundTask int
	node      map[string]int
}

// NewGroup create a new empty bucket
func NewGroup(index int) *Group {
	return &Group{
		index:       index,
		tasks:       make(map[types.UID]*api.TaskInfo),
		taskNameSet: make(map[string]int),

		reqScore: 0,
		request:  api.EmptyResource(),

		boundTask: 0,
		node:      make(map[string]int),
	}
}

type SortByScore []*Group

func (s SortByScore) Len() int {
	return len(s)
}

func (s SortByScore) Less(i, j int) bool {
	return s[i].reqScore < s[j].reqScore
}

func (s SortByScore) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type SortByIndex []*Group

func (s SortByIndex) Len() int {
	return len(s)
}

func (s SortByIndex) Less(i, j int) bool {
	return s[i].index < s[j].index
}

func (s SortByIndex) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// CalcResReq calculates task resources request
func (g *Group) CalcResReq(req *api.Resource, action reqAction) {
	if req == nil {
		return
	}

	cpu := req.MilliCPU
	// treat 1Mi the same as 1m cpu 1m gpu
	//mem := req.Memory / 1024 / 1024
	//score := cpu + mem
	score := cpu
	// for _, request := range req.ScalarResources {
	// 	score += request
	// }

	switch action {
	case reqSub:
		g.reqScore -= score
		g.request.Sub(req)
	case reqAdd:
		g.reqScore += score
		g.request.Add(req)
	default:
		klog.V(3).Infof("Invalid action <%v> for resource <%v>", action, req)
	}
}

// AddTask adds task into group
func (g *Group) AddTask(taskName string, task *api.TaskInfo) {
	g.taskNameSet[taskName]++
	if task.NodeName != "" {
		g.node[task.NodeName]++
		g.boundTask++
		return
	}

	g.tasks[task.Pod.UID] = task
	g.CalcResReq(task.Resreq, reqAdd)
	// klog.Infof("addTask: group=%s, taskreq %s, groupscore %s,  taskset %d",
	// 	g.index, task.Resreq.MilliCPU, g.reqScore, g.taskNameSet[taskName])
}

// TaskBound binds task to group
func (g *Group) TaskBound(task *api.TaskInfo) {
	g.node[task.NodeName]++
	g.boundTask++

	delete(g.tasks, task.Pod.UID)
	g.CalcResReq(task.Resreq, reqSub)
}
