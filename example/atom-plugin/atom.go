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

package main // note!!! package must be named main

import (
	"strconv"

	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
)

const PluginName = "atom"

type atomPlugin struct{}

func (ap *atomPlugin) Name() string {
	return PluginName
}

// New is a PluginBuilder, remove the comment when used.
func New(arguments framework.Arguments) framework.Plugin {
	return &atomPlugin{}
}

func getPodGroupCustomOrder(podgroup *api.PodGroup) int {
	if podgroup.Annotations == nil {
		klog.V(2).Infof("podgroup %v has no annotation", podgroup.Name)
		return -1
	}
	v, ok := podgroup.Annotations["atom.supremind.com/customer-order"]
	if !ok {
		return -1
	} else {
		customOrder, err := strconv.Atoi(v)
		if err != nil {
			klog.V(2).Infof("atom.supremind.com/customer-order annotation is not a number")
			return -1
		} else {
			return customOrder
		}
	}
	return -1
}

func getPodGroupTenant(podgroup *api.PodGroup) string {
	if podgroup.Labels == nil {
		klog.V(2).Infof("podgroup %v has no annotation", podgroup.Name)
		return ""
	}
	v, ok := podgroup.Labels["atom.supremind.com/tenant"]
	if !ok {
		return ""
	} else {
		return v
	}
	return ""
}
func (ap *atomPlugin) OnSessionOpen(ssn *framework.Session) {
	jobOrderFn := func(l, r interface{}) int {
		lv := l.(*api.JobInfo)
		rv := r.(*api.JobInfo)

		klog.V(2).Infof("Priority JobOrderFn: <%v/%v> priority: %d, <%v/%v> priority: %d",
			lv.Namespace, lv.Name, lv.Priority, rv.Namespace, rv.Name, rv.Priority)

		lcustomOrder := getPodGroupCustomOrder(lv.PodGroup)
		rcustomOrder := getPodGroupCustomOrder(rv.PodGroup)

		if lcustomOrder > rcustomOrder {
			return 1
		}

		if lcustomOrder < rcustomOrder {
			return -1
		}

		if lv.Priority > rv.Priority {
			return -1
		}

		if lv.Priority < rv.Priority {
			return 1
		}

		return 0
	}

	ssn.AddJobOrderFn(ap.Name(), jobOrderFn)

	jobEnqueueFn := func(obj interface{}) int {
		klog.V(2).Infof("dkk priority enqueueable,jobs length:%v", len(ssn.Jobs))
		job := obj.(*api.JobInfo)
		queueID := job.Queue
		for _, queue_job := range ssn.Jobs {
			klog.V(2).Infof("jobName:%v,queueID:%v,queue_job.Queue:%v,job status:%v", queue_job.Name, queueID, queue_job.Queue, queue_job.PodGroup.Status.Phase)
			if queue_job.Queue == queueID {
				tenant_queue_job := getPodGroupTenant(queue_job.PodGroup)
				tenant_job := getPodGroupTenant(job.PodGroup)
				if queue_job.PodGroup.Status.Phase == scheduling.PodGroupInqueue && tenant_queue_job == tenant_job {
					klog.V(2).Infof("jobName:%v rejected", queue_job.Name)
					return util.Reject
				}
			}
		}
		return util.Abstain
	}
	ssn.AddJobEnqueueableFn(ap.Name(), jobEnqueueFn)

}

func (ap *atomPlugin) OnSessionClose(ssn *framework.Session) {}
