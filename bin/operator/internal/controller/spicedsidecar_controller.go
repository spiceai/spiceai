/*
Copyright 2024 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// SpicedSidecarReconciler reconciles a SpicedSidecar object
type SpicedSidecarReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

const (
	sidecarContainerName = "spiceai-sidecar"
	defaultSpicepod      = `version: v1beta1
kind: Spicepod
name: spicepod-quickstart

secrets:
  store: env

datasets:
  - from: s3://spiceai-demo-datasets/taxi_trips/2024/
    name: taxi_trips
    params:
      file_format: parquet`
)

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="apps",resources=deployments,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the SpicedSidecar object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.18.2/pkg/reconcile
func (r *SpicedSidecarReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	deployment := &appsv1.Deployment{}
	err := r.Get(ctx, req.NamespacedName, deployment)
	if err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}

	podSpec := &deployment.Spec.Template.Spec
	podAnnotations := deployment.Spec.Template.Annotations

	sidecarContainer := getSidecarContainer(podSpec)

	// if pod annotated and has no sidecar container
	if podAnnotations["spiceai.org/enabled"] == "true" && sidecarContainer == nil {
		configMapName := fmt.Sprintf("%s-spiceai-config", deployment.Name)
		configMap := &corev1.ConfigMap{}
		err = r.Get(ctx, client.ObjectKey{Namespace: deployment.Namespace, Name: configMapName}, configMap)
		if err != nil {
			if client.IgnoreNotFound(err) == nil {
				configMap = &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      configMapName,
						Namespace: deployment.Namespace,
					},
					Data: map[string]string{
						"spicepod.yaml": defaultSpicepod,
					},
				}

				err = r.Create(ctx, configMap)
				if err != nil {
					logger.Error(err, "Failed to create ConfigMap", "configmap", configMapName)
					return ctrl.Result{}, err
				}

				logger.Info("Spiced ConfigMap created", "configmap", configMapName)
			} else {
				logger.Error(err, "Failed to get ConfigMap", "configmap", configMapName)
				return ctrl.Result{}, err
			}
		}

		err = r.Update(ctx, configMap)
		if err != nil {
			logger.Error(err, "Failed to create ConfigMap", "configmap", configMapName)
			return ctrl.Result{}, err
		}

		logger.Info("Spiced ConfigMap created", "configmap", configMapName)

		podSpec.Containers = append(podSpec.Containers, corev1.Container{
			Name:  sidecarContainerName,
			Image: "spiceai/spiceai",
			Ports: []corev1.ContainerPort{
				{
					ContainerPort: 3000,
					Name:          "http-port",
					Protocol:      corev1.ProtocolTCP,
				},
				{
					ContainerPort: 50051,
					Name:          "grpc-port",
					Protocol:      corev1.ProtocolTCP,
				},
			},
			VolumeMounts: []corev1.VolumeMount{
				{
					Name:      "spiceai-config-volume",
					MountPath: "/app/spicepod.yaml", // Mount to /app/spicepod.yaml
					SubPath:   "spicepod.yaml",      // Use subPath to specify the file name
				},
			},
		})

		podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
			Name: "spiceai-config-volume",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: configMap.Name,
					},
				},
			},
		})

		err = r.Update(ctx, deployment)
		if err != nil {
			logger.Error(err, "Failed to update Pod", "pod", deployment.Name)
			return ctrl.Result{}, err
		}

		logger.Info("SpiceAI sidecar container injected", "pod", deployment.Name)
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *SpicedSidecarReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&appsv1.Deployment{}).
		Complete(r)
}

func getSidecarContainer(podSpec *corev1.PodSpec) *corev1.Container {
	for _, container := range podSpec.Containers {
		if container.Name == sidecarContainerName {
			return &container
		}
	}

	return nil
}
