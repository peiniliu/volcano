/*
Copyright 2019 The Volcano Authors.

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

package mpi

import (
	"flag"
	"fmt"
	"math"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog"

	"volcano.sh/apis/pkg/apis/batch/v1alpha1"
	batch "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	"volcano.sh/apis/pkg/apis/helpers"
	jobhelpers "volcano.sh/volcano/pkg/controllers/job/helpers"
	pluginsinterface "volcano.sh/volcano/pkg/controllers/job/plugins/interface"
)

type mpiPlugin struct {
	// Arguments given for the plugin
	pluginArguments []string

	Clientset pluginsinterface.PluginClientset

	//name of master/worker
	masterName string
	workerName string

	//oversubscribe
	oversubscribe bool

	//number of total tasks of mpi application, same as -np
	nTasks int
	//number of nodes to distribute the workers
	//nNodes int
	//number of workers
	nWorkers int

	//number of tasks per worker can be calculated by nTasks/nWorkers if exact subscribe
	nTasksPerWorker int
	nCpusPerTask    int
	memoryPerTask   string
}

// New creates service plugin.
func New(client pluginsinterface.PluginClientset, arguments []string) pluginsinterface.PluginInterface {
	mpiPlugin := mpiPlugin{pluginArguments: arguments, Clientset: client}

	mpiPlugin.addFlags()

	return &mpiPlugin
}

func (mp *mpiPlugin) Name() string {
	return "mpi"
}

func (mp *mpiPlugin) addFlags() {
	flagSet := flag.NewFlagSet(mp.Name(), flag.ContinueOnError)
	flagSet.StringVar(&mp.masterName, "masterName", "mpimaster", "mpi master task name")
	flagSet.StringVar(&mp.workerName, "workerName", "mpiworker", "mpi worker task name")

	flagSet.BoolVar(&mp.oversubscribe, "oversubscribe", false, "set oversubscribe to false")

	flagSet.IntVar(&mp.nTasks, "nTasks", 1, "by default is 1.")
	flagSet.IntVar(&mp.nWorkers, "nWorkers", 1, "by default is 1.")
	//flagSet.IntVar(&mp.nNodes, "nNodes", 1, "by default is 1.")

	flagSet.IntVar(&mp.nTasksPerWorker, "nTasksPerWorker", int(math.Ceil(float64(mp.nTasks/mp.nWorkers))),
		"max tasks per worker if used with nTasks and nWorkers, by default is ceil mp.nTasks/mp.nWorkers.")
	flagSet.IntVar(&mp.nCpusPerTask, "nCpusPerTask", 1, "by default is 1.")
	flagSet.StringVar(&mp.memoryPerTask, "memoryPerTask", "5Gi", "by default is 5Gi.")
	//flagSet.IntVar(&mp.nTasksPerWorker, "nTasksPerWorker", 0, "by default is 0.")
	//flagSet.IntVar(&mp.nWorkersPerNode, "nWorkersPerNode", 1, "by default is 1.")

	if err := flagSet.Parse(mp.pluginArguments); err != nil {
		klog.Errorf("plugin %s flagset parse failed, err: %v", mp.Name(), err)
	} else {
		klog.Infof("mpi plugin parameters : masterName:%s, workerName:%s, nTasks:%s, nWorkers:%s, nTasksPerWorker:%s, nCpusPerTask:%s",
			mp.masterName, mp.workerName, mp.nTasks, mp.nWorkers, mp.nTasksPerWorker, mp.nCpusPerTask)
	}
}

func (mp *mpiPlugin) OnPodCreate(pod *v1.Pod, job *batch.Job) error {

	mp.mountConfigmap(pod, job)
	mp.calcPodResource(pod, job)

	return nil
}

func (mp *mpiPlugin) OnJobAdd(job *batch.Job) error {
	if job.Status.ControlledResources["plugin-"+mp.Name()] == mp.Name() {
		return nil
	}

	hostFile := mp.GenerateHostFile(job)

	// Create ConfigMap of hosts for Pods to mount.
	if err := helpers.CreateOrUpdateConfigMap(job, mp.Clientset.KubeClients, hostFile, mp.cmName(job)); err != nil {
		return err
	}

	job.Status.ControlledResources["plugin-"+mp.Name()] = mp.Name()

	return nil
}

func (mp *mpiPlugin) OnJobDelete(job *batch.Job) error {
	if job.Status.ControlledResources["plugin-"+mp.Name()] != mp.Name() {
		return nil
	}

	if err := helpers.DeleteConfigmap(job, mp.Clientset.KubeClients, mp.cmName(job)); err != nil {
		return err
	}

	delete(job.Status.ControlledResources, "plugin-"+mp.Name())

	return nil
}

//when scale up or down a job
//mpi job can not be scaled during the runtime, because it will restart
func (mp *mpiPlugin) OnJobUpdate(job *batch.Job) error {
	return nil
}

// func (mp *mpiPlugin) calcTaskReplicas(job *batch.Job) *batch.Job {
// 	//TODO: now only consider exactsubscribe
// 	if !mp.oversubscribe {
// 		jobCharacteristic := job.Annotations[batch.JobCharacteristicKey]
// 		for i, ts := range job.Spec.Tasks {
// 			if ts.Name == mp.workerName {
// 				if mp.granularity {
// 					switch jobCharacteristic {
// 					case "network":
// 						job.Spec.Tasks[i].Replicas = int32(mp.nNodes)
// 					case "cpu":
// 						job.Spec.Tasks[i].Replicas = int32(mp.nTasks)
// 					case "memory":
// 						job.Spec.Tasks[i].Replicas = int32(mp.nTasks)
// 					}
// 					mp.nWorkers = int(job.Spec.Tasks[i].Replicas)
// 				} else {
// 					job.Spec.Tasks[i].Replicas = int32(mp.nNodes)
// 					mp.nWorkers = int(job.Spec.Tasks[i].Replicas)
// 					klog.Infof("cal -%v-------------%v----", mp.nNodes, mp.nWorkers)
// 				}
// 			}
// 		}
// 	}
// 	return job
// }

func (mp *mpiPlugin) calcPodResource(pod *v1.Pod, job *batch.Job) {
	//TODO: now only consider exactsubscribe
	if !mp.oversubscribe {

		if pod.Annotations[v1alpha1.TaskSpecKey] == mp.workerName {
			indexStr := jobhelpers.GetPodIndexUnderTask(pod)
			index, _ := strconv.Atoi(indexStr)
			nTasksInWorker := mp.getnTasksInWorker()
			klog.Infof("pod : index:%s taskinwork:%s", indexStr, nTasksInWorker)

			var memorySize resource.Quantity
			for i := 0; i < nTasksInWorker[index]; i++ {
				memorySize.Add(resource.MustParse(mp.memoryPerTask))
			}
			resourceRequirements := v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse(fmt.Sprint(nTasksInWorker[index] * mp.nCpusPerTask)),
					v1.ResourceMemory: memorySize,
				},
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse(fmt.Sprint(nTasksInWorker[index] * mp.nCpusPerTask)),
					v1.ResourceMemory: memorySize,
				},
			}

			for i, _ := range pod.Spec.Containers {
				pod.Spec.Containers[i].Resources = resourceRequirements
			}
		} else if pod.Annotations[v1alpha1.TaskSpecKey] == mp.masterName {
			var memorySize resource.Quantity
			var cpuSize resource.Quantity
			for i := 0; i < mp.nTasks; i++ {
				memorySize.Add(resource.MustParse("5Mi"))
				cpuSize.Add(resource.MustParse("5m"))
			}
			resourceRequirements := v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceCPU:    cpuSize,
					v1.ResourceMemory: memorySize,
				},
				Requests: v1.ResourceList{
					v1.ResourceCPU:    cpuSize,
					v1.ResourceMemory: memorySize,
				},
			}
			for i, _ := range pod.Spec.Containers {
				pod.Spec.Containers[i].Resources = resourceRequirements
			}
		}

	}
}

func (mp *mpiPlugin) mountConfigmap(pod *v1.Pod, job *batch.Job) {
	cmName := mp.cmName(job)
	cmVolume := v1.Volume{
		Name: cmName,
	}
	cmVolume.ConfigMap = &v1.ConfigMapVolumeSource{
		LocalObjectReference: v1.LocalObjectReference{
			Name: cmName,
		},
	}

	pod.Spec.Volumes = append(pod.Spec.Volumes, cmVolume)

	vm := v1.VolumeMount{
		MountPath: ConfigMapMountPath,
		Name:      cmName,
	}

	for i, c := range pod.Spec.Containers {
		pod.Spec.Containers[i].VolumeMounts = append(c.VolumeMounts, vm)
	}
	for i, c := range pod.Spec.InitContainers {
		pod.Spec.InitContainers[i].VolumeMounts = append(c.VolumeMounts, vm)
	}
}

func (mp *mpiPlugin) cmName(job *batch.Job) string {
	return fmt.Sprintf("%s-%s", job.Name, mp.Name())
}

func (mp *mpiPlugin) getnTasksInWorker() []int {
	//task to worker in roundrobin
	nTasksInWorker := make([]int, mp.nWorkers)
	x := 0
	for j := 0; j < int(mp.nTasks); j++ {
		nTasksInWorker[x]++
		x++
		if x >= mp.nWorkers {
			x = 0
		}
	}
	return nTasksInWorker
}

func (mp *mpiPlugin) GenerateHostFile(job *batch.Job) map[string]string {

	hostFile := make(map[string]string, len(job.Spec.Tasks))

	nTasksInWorker := mp.getnTasksInWorker()

	for _, ts := range job.Spec.Tasks {
		// workers to hostfile
		if ts.Name == mp.workerName {
			hosts := make([]string, 0, ts.Replicas)

			for i := 0; i < int(ts.Replicas); i++ {
				hostName := ts.Template.Spec.Hostname
				subdomain := ts.Template.Spec.Subdomain
				if len(hostName) == 0 {
					hostName = jobhelpers.MakePodName(job.Name, ts.Name, i)
				}
				if len(subdomain) == 0 {
					subdomain = job.Name
				}
				hosts = append(hosts, hostName+"."+subdomain+" slots="+fmt.Sprint(nTasksInWorker[i]*mp.nCpusPerTask))
				if len(ts.Template.Spec.Hostname) != 0 {
					break
				}
			}

			hostFile[HostFile] = strings.Join(hosts, "\n")
			klog.Infof("mpi plugin parameters : nTasksPerWorker:%s", nTasksInWorker)
		}

	}

	return hostFile
}
