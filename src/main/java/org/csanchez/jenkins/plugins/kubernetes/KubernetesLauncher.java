/*
 * The MIT License
 *
 * Copyright (c) 2017, CloudBees, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package org.csanchez.jenkins.plugins.kubernetes;

import static java.util.logging.Level.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.annotation.CheckForNull;

import org.apache.commons.lang.StringUtils;
import org.jenkinsci.plugins.workflow.graph.FlowNode;
import org.jenkinsci.plugins.workflow.support.steps.ExecutorStepExecution;
import org.kohsuke.stapler.DataBoundConstructor;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import antlr.ANTLRException;
import hudson.model.Label;
import hudson.model.Queue;
import hudson.model.TaskListener;
import hudson.model.Queue.BuildableItem;
import hudson.model.Queue.NotWaitingItem;
import hudson.model.labels.LabelAtom;
import hudson.slaves.JNLPLauncher;
import hudson.slaves.SlaveComputer;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HostPathVolumeSource;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.fabric8.kubernetes.client.dsl.PrettyLoggable;
import io.fabric8.kubernetes.client.utils.KubernetesResourceUtil;
import jenkins.model.Jenkins;

/**
 * Launches on Kubernetes the specified {@link KubernetesComputer} instance.
 */
public class KubernetesLauncher extends JNLPLauncher {

    @CheckForNull
    private transient AllContainersRunningPodWatcher watcher;

    private static final Logger LOGGER = Logger.getLogger(KubernetesLauncher.class.getName());

    static final String JOB_NAME_LABEL = "job-name";
    private static final String PIPELINE_JOB_URL_SUFFIX = "/[0-9]+/$";

    private boolean launched;

    /**
     * Provisioning exception if any.
     */
    @CheckForNull
    private transient Throwable problem;

    @DataBoundConstructor
    public KubernetesLauncher(String tunnel, String vmargs) {
        super(tunnel, vmargs);
    }

    public KubernetesLauncher() {
        super();
    }

    @Override
    public boolean isLaunchSupported() {
        return !launched;
    }

    @Override
    public void launch(SlaveComputer computer, TaskListener listener) {

        if (!(computer instanceof KubernetesComputer)) {
            throw new IllegalArgumentException("This Launcher can be used only with KubernetesComputer");
        }
        KubernetesComputer kubernetesComputer = (KubernetesComputer) computer;
        computer.setAcceptingTasks(false);
        KubernetesSlave slave = kubernetesComputer.getNode();
        if (slave == null) {
            throw new IllegalStateException("Node has been removed, cannot launch " + computer.getName());
        }
        if (launched) {
            LOGGER.log(INFO, "Agent has already been launched, activating: {}", slave.getNodeName());
            computer.setAcceptingTasks(true);
            return;
        }

        if (Jenkins.get().isQuietingDown() || Jenkins.get().isTerminating()) {
            throw new IllegalStateException("Jenkins is qieting down or terminating, no new agent will be provisioned");
        }

        final PodTemplate template = slave.getTemplate();
        try {
            KubernetesClient client = slave.getKubernetesCloud().connect();
            Pod pod = template.build(slave);
            if (template.isShowRawYaml()) {
                slave.assignPod(pod);
            }

            String podId = pod.getMetadata().getName();

            String namespace = Arrays.asList( //
                    pod.getMetadata().getNamespace(),
                    template.getNamespace(), client.getNamespace()) //
                    .stream().filter(s -> StringUtils.isNotBlank(s)).findFirst().orElse(null);
            slave.setNamespace(namespace);

            // get current buildable item from the global build queue and provision for it
            Queue queue = Jenkins.get().getQueue();
            List<BuildableItem> buildables = queue.getBuildableItems();

            // KubernetesLauncher.launch() gets called concurrently
            // ensure that checking for the first unprovisioned job and creating a pod for it
            // is done atomically
            synchronized (KubernetesLauncher.class) {

                BuildableItem buildable = findFirstBuildableToProvisionFor(buildables, namespace,
                        template.getLabel(), client);
                if (buildable == null) {
                    // we must throw an exception here, a simple return leads to a zombie slave
                    throw new IllegalStateException("No unprovisioned job found in queue");
                }

                String podLabel = calcPodLabel(buildable);
                if (!isPipelineJob(buildable)) {
                    //set the label string early, so that Jenkins can provision for other jobs earlier
                    slave.setLabelString(podLabel);
                }
                // adjust pod dynamically to the job that gets scheduled to it
                adjustPodToBuildable(pod, buildable, namespace);

                LOGGER.log(Level.FINE, "Creating Pod: {0} in namespace {1}", new Object[]{podId, namespace});
                pod = client.pods().inNamespace(namespace).create(pod);
                LOGGER.log(INFO, "Created Pod: {0} in namespace {1}", new Object[]{podId, namespace});
                listener.getLogger().printf("Created Pod: %s in namespace %s%n", podId, namespace);
            }

            String podName = pod.getMetadata().getName();
            String namespace1 = pod.getMetadata().getNamespace();
            watcher = new AllContainersRunningPodWatcher(client, pod);
            try (Watch _w = client.pods().inNamespace(namespace1).withName(podName).watch(watcher)) {
                watcher.await(template.getSlaveConnectTimeout(), TimeUnit.SECONDS);
            }
            LOGGER.log(INFO, "Pod is running: {0}/{1}", new Object[] { namespace, podId });

            // We need the pod to be running and connected before returning
            // otherwise this method keeps being called multiple times
            List<String> validStates = ImmutableList.of("Running");

            int waitForSlaveToConnect = template.getSlaveConnectTimeout();
            int waitedForSlave;

            // now wait for agent to be online
            SlaveComputer slaveComputer = null;
            String status = null;
            List<ContainerStatus> containerStatuses = null;
            for (waitedForSlave = 0; waitedForSlave < waitForSlaveToConnect; waitedForSlave++) {
                slaveComputer = slave.getComputer();
                if (slaveComputer == null) {
                    throw new IllegalStateException("Node was deleted, computer is null");
                }
                if (slaveComputer.isOnline()) {
                    break;
                }

                // Check that the pod hasn't failed already
                pod = client.pods().inNamespace(namespace).withName(podId).get();
                if (pod == null) {
                    throw new IllegalStateException("Pod no longer exists: " + podId);
                }
                status = pod.getStatus().getPhase();
                if (!validStates.contains(status)) {
                    break;
                }

                containerStatuses = pod.getStatus().getContainerStatuses();
                List<ContainerStatus> terminatedContainers = new ArrayList<>();
                for (ContainerStatus info : containerStatuses) {
                    if (info != null) {
                        if (info.getState().getTerminated() != null) {
                            // Container has errored
                            LOGGER.log(INFO, "Container is terminated {0} [{2}]: {1}",
                                    new Object[] { podId, info.getState().getTerminated(), info.getName() });
                            listener.getLogger().printf("Container is terminated %1$s [%3$s]: %2$s%n", podId,
                                    info.getState().getTerminated(), info.getName());
                            terminatedContainers.add(info);
                        }
                    }
                }

                checkTerminatedContainers(terminatedContainers, podId, namespace, slave, client);

                LOGGER.log(INFO, "Waiting for agent to connect ({1}/{2}): {0}",
                        new Object[] { podId, waitedForSlave, waitForSlaveToConnect });
                listener.getLogger().printf("Waiting for agent to connect (%2$s/%3$s): %1$s%n", podId, waitedForSlave,
                        waitForSlaveToConnect);
                Thread.sleep(1000);
            }
            if (slaveComputer == null || slaveComputer.isOffline()) {
                logLastLines(containerStatuses, podId, namespace, slave, null, client);
                throw new IllegalStateException(
                        "Agent is not connected after " + waitedForSlave + " seconds, status: " + status);
            }

            computer.setAcceptingTasks(true);
            launched = true;
            try {
                // We need to persist the "launched" setting...
                slave.save();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Could not save() agent: " + e.getMessage(), e);
            }
        } catch (Throwable ex) {
            setProblem(ex);
            LOGGER.log(Level.WARNING, String.format("Error in provisioning; agent=%s, template=%s", slave, template), ex);
            LOGGER.log(Level.FINER, "Removing Jenkins node: {0}", slave.getNodeName());
            try {
                slave.terminate();
            } catch (IOException | InterruptedException e) {
                LOGGER.log(Level.WARNING, "Unable to remove Jenkins node", e);
            }
            throw Throwables.propagate(ex);
        }
    }

    BuildableItem findFirstBuildableToProvisionFor(List<BuildableItem> buildables, String namespace,
            String templateLabel, KubernetesClient client) {

        Set<LabelAtom> templateLabelSet = Label.parse(templateLabel);

        for (BuildableItem buildable : buildables) {
            String podLabel = calcPodLabel(buildable);
            LOGGER.log(Level.FINE, "checking if buildable {0} should be provisioned", podLabel);

            if (buildable instanceof NotWaitingItem) {
                //this item is not waiting
                LOGGER.log(Level.FINEST, "buildable {0} is not waiting", podLabel);
                if (KubernetesCloud.filterRunningOrPendingAgentPods(client.pods()
                        .inNamespace(namespace)
                        .withLabel(JOB_NAME_LABEL, podLabel)
                        .list())
                        .isEmpty()) {
                    LOGGER.log(Level.FINEST, "buildable {0} has no other active pod running", podLabel);
                    //no other active pod exists for this job
                    Label labelExpression = parseLabelExpression(buildable.task.getAssignedLabel().getExpression());
                    if (labelExpression != null && labelExpression.matches(templateLabelSet)) {
                        //the template label satisfies the job's label expression, provision
                        LOGGER.log(Level.FINE, "buildable {0} has all criteria true, provision!", podLabel);
                        return buildable;
                    }
                }
            }
        }
        return null;
    }

    void adjustPodToBuildable(Pod pod, BuildableItem buildable, String namespace) {
        String podLabel = calcPodLabel(buildable);
        String workspacePath = calcWorkspacePath(buildable);
        LOGGER.log(INFO, "podLabel : {0}, workspacePath: {1}", new Object[] {podLabel, workspacePath});

        // label pod with job name
        pod.getMetadata().getLabels().put(JOB_NAME_LABEL, podLabel);

        Container jnlp = findJnlpContainer(pod.getSpec().getContainers());
        if (jnlp != null) {
            // add full job name as environment variable for script inside the agent container
            jnlp.getEnv().add(new EnvVarBuilder().withName("WORKSPACE_PATH").withValue(workspacePath).build());
            // adjust the workspace directory on the host to contain a directory for the namespace and the job name
            adjustWorkspaceVolume(pod, podLabel, namespace);
        } else {
            LOGGER.log(WARNING, "could not find jnlp container volume to adjust : {0}", pod);
        }
    }

    void adjustWorkspaceVolume(Pod pod, String jobName, String namespace) {
        // find the HostPath workspace volume
        HostPathVolumeSource hostPath = findWorkspaceVolumeSource(pod);
        if (hostPath != null) {
            // adjust HostPath workspace volume for specific job
            hostPath.setPath(hostPath.getPath() + "/" + namespace + "/" + jobName);
            LOGGER.log(FINE, "adjusted hostpath volume : {0}", hostPath);
        } else {
            LOGGER.log(WARNING, "could not find hostpath volume to adjust : {0}", hostPath);
        }
    }

    HostPathVolumeSource findWorkspaceVolumeSource(Pod pod) {
        List<Volume> volumes = pod.getSpec().getVolumes();
        for (Volume vol : volumes) {
            HostPathVolumeSource hostPath = vol.getHostPath();
            if (vol.getName().equals("volume-0") && hostPath != null) {
                // this is the first hostPath volume of the pod, it's the workspace volume
                // by convention
                LOGGER.log(FINE, "found job host path: {0}", hostPath);
                return hostPath;
            }
        }
        return null;
    }

    boolean isPipelineJob(BuildableItem buildable) {
        return buildable.task instanceof ExecutorStepExecution.PlaceholderTask;
    }

    String calcWorkspacePath(BuildableItem buildable) {
        String name = buildable.task.getUrl().replace("job/", "");
        if (isPipelineJob(buildable)) {
            // this is a PipelineJob, adjust name further
            name = name.replaceFirst(PIPELINE_JOB_URL_SUFFIX, "");
        }
        return name;
    }

    private String getNodeId(BuildableItem buildable) {
        String nodeId = "unknownNodeId";
        FlowNode node = null;
        try {
            node = ((ExecutorStepExecution.PlaceholderTask) buildable.task).getNode();
            if (node != null) {
                nodeId = node.getId();
            }
        } catch (IOException | InterruptedException e) {
            // ignore
        }
        return nodeId;
    }

    String calcPodLabel(BuildableItem buildable) {
        String[] urlParts = buildable.task.getUrl().split("/");
        String podLabel = null;
        if (isPipelineJob(buildable)) {
            podLabel = urlParts[urlParts.length - 2] + "-" + urlParts[urlParts.length - 1] + "-" + getNodeId(buildable);
        } else {
            podLabel = urlParts[urlParts.length - 1];
        }
        return KubernetesResourceUtil.sanitizeName(podLabel);
    }

    Container findJnlpContainer(List<Container> containers) {
        for (Container c : containers) {
            if (c.getName().equals(KubernetesCloud.JNLP_NAME)) {
                return c;
            }
        }
        return null;
    }

    private Label parseLabelExpression(String labelString) {
        Label result = null;
        try {
            result = Label.parseExpression(labelString);
        } catch (ANTLRException e) {
            LOGGER.log(Level.SEVERE, "error parsing label string {0}", labelString);
        }
        return result;
    }

    private void checkTerminatedContainers(List<ContainerStatus> terminatedContainers, String podId, String namespace,
            KubernetesSlave slave, KubernetesClient client) {
        if (!terminatedContainers.isEmpty()) {
            Map<String, Integer> errors = terminatedContainers.stream().collect(Collectors
                    .toMap(ContainerStatus::getName, (info) -> info.getState().getTerminated().getExitCode()));

            // Print the last lines of failed containers
            logLastLines(terminatedContainers, podId, namespace, slave, errors, client);
            throw new IllegalStateException("Containers are terminated with exit codes: " + errors);
        }
    }

    /**
     * Log the last lines of containers logs
     */
    private void logLastLines(List<ContainerStatus> containers, String podId, String namespace, KubernetesSlave slave,
            Map<String, Integer> errors, KubernetesClient client) {
        for (ContainerStatus containerStatus : containers) {
            String containerName = containerStatus.getName();
            PrettyLoggable<String, LogWatch> tailingLines = client.pods().inNamespace(namespace).withName(podId)
                    .inContainer(containerStatus.getName()).tailingLines(30);
            String log = tailingLines.getLog();
            if (!StringUtils.isBlank(log)) {
                String msg = errors != null ? String.format(" exited with error %s", errors.get(containerName)) : "";
                LOGGER.log(Level.SEVERE, "Error in provisioning; agent={0}, template={1}. Container {2}{3}. Logs: {4}",
                        new Object[] { slave, slave.getTemplate(), containerName, msg, tailingLines.getLog() });
            }
        }
    }

    /**
     * The last problem that occurred, if any.
     * @return
     */
    @CheckForNull
    public Throwable getProblem() {
        return problem;
    }

    public void setProblem(@CheckForNull Throwable problem) {
        this.problem = problem;
    }

    public AllContainersRunningPodWatcher getWatcher() {
        return watcher;
    }

}
