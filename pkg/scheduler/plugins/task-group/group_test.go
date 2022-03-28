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
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
)

func Test_readTaskGroupFromPgAnnotations(t *testing.T) {
	cases := []struct {
		description string
		job         *api.JobInfo
		tsgroup     *TaskGroup
		err         error
	}{
		{
			description: "correct annotation",
			job: &api.JobInfo{
				Name:      "job1",
				Namespace: "default",
				Tasks: map[api.TaskID]*api.TaskInfo{
					"0": {
						Name: "job1-ps-0",
					},
					"1": {
						Name: "job1-ps-1",
					},
					"2": {
						Name: "job1-worker-0",
					},
					"3": {
						Name: "job1-worker-1",
					},
				},
				PodGroup: &api.PodGroup{
					PodGroup: scheduling.PodGroup{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{
								taskGroupAnnotations: "worker:4",
							},
						},
					},
				},
			},
			tsgroup: &TaskGroup{
				TaskOrder: []string{
					"worker",
				},
				Groups: map[string]int{
					"worker": 4,
				},
			},
			err: nil,
		},
		{
			description: "nil annotation",
			job: &api.JobInfo{
				PodGroup: &api.PodGroup{
					PodGroup: scheduling.PodGroup{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: nil,
						},
					},
				},
			},
			tsgroup: nil,
			err:     nil,
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
			t.Logf("case: %s", c.description)
			tsgroup, err := readTaskGroupFromPgAnnotations(c.job)
			if !reflect.DeepEqual(err, c.err) {
				t.Errorf("want %v ,got %v", c.err, err)
			}
			if !reflect.DeepEqual(tsgroup, c.tsgroup) {
				t.Errorf("want %v ,got %v", c.tsgroup, tsgroup)
			}
		})
	}
}
