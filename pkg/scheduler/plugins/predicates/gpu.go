/*
Copyright 2020 The Kubernetes Authors.

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

package predicates

import (
	"fmt"
    "sort"

	v1 "k8s.io/api/core/v1"

	"volcano.sh/volcano/pkg/scheduler/api"
)

type GPUStatus struct {
    Id  int
    RemainMemory uint
}

type GPUStatusList []GPUStatus

func (g GPUStatusList) Len() int           { return len(g) }
func (g GPUStatusList) Swap(i, j int)      { g[i], g[j] = g[j], g[i] }
func (g GPUStatusList) Less(i, j int) bool { return g[i].RemainMemory < g[j].RemainMemory }


//sort
func sortGPUByIdleMemory(allocatableGPUs map[int]uint) GPUStatusList {
	 gpus := make(GPUStatusList, len(allocatableGPUs))
	 i := 0
	 for k, v := range allocatableGPUs {
		 gpus[i] = GPUStatus{k, v}
		 i++
	 }
	 sort.Sort(gpus) //sort with values
	 //var ids []int
	 //for _, gpu := range gpus {
	//	 ids = append(ids, gpu.Id)
	 //}
	return gpus
}


// checkNodeGPUSharingPredicate checks if a pod with gpu requirement can be scheduled on a node.
func checkNodeGPUSharingPredicate(pod *v1.Pod, nodeInfo *api.NodeInfo) (bool, error) {
	// no gpu sharing request
	if api.GetGPUResourceOfPod(pod) <= 0 && api.GetGPUNumberOfPod(pod) <=0 {
		return true, nil
	}
	ids := predicateGPU(pod, nodeInfo)
	if ids == nil {
		return false, fmt.Errorf("no enough gpu cards/memory on node %s", nodeInfo.Name)
	}
	return true, nil
}

// predicateGPU returns the available GPU IDs
func predicateGPU(pod *v1.Pod, node *api.NodeInfo) []int {
	gpuNumberRequest := api.GetGPUNumberOfPod(pod)
	gpuMemoryRequest := api.GetGPUResourceOfPod(pod)
	allocatableGPUs := node.GetDevicesIdleGPUMemory()

	//no gpu
	if allocatableGPUs == nil {
       return nil
	}
	//1. sort gpu by idlememory
    sortedGPU := sortGPUByIdleMemory(allocatableGPUs)

	var devIDs []int
    var availableMemoryTotal uint = 0
	if len(allocatableGPUs) < gpuNumberRequest {
		return nil
	} 
    for index, gpu := range sortedGPU {
        //2. preallocate required number of gpus
		devIDs = append(devIDs, gpu.Id)
        availableMemoryTotal += gpu.RemainMemory
		if len(devIDs) == gpuNumberRequest{
		   //3 & 4. memory check
	       if availableMemoryTotal >= gpuMemoryRequest {
               return devIDs;
	        }
            devIDs = devIDs[1:]//dequeue		   
            availableMemoryTotal -= allocatableGPUs[devIDs[0]]
		}
    }
		
	return nil
}
