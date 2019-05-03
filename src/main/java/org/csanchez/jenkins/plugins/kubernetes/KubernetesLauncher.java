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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.CheckForNull;

import jenkins.model.Jenkins;

import org.apache.commons.lang.StringUtils;
import org.kohsuke.stapler.DataBoundConstructor;

import com.google.common.base.Throwables;

import antlr.ANTLRException;
import hudson.model.Queue.BuildableItem;
import hudson.model.Queue.NotWaitingItem;
import hudson.model.labels.LabelAtom;
import hudson.model.Label;
import hudson.model.Queue;
import hudson.model.TaskListener;
import hudson.slaves.JNLPLauncher;
import hudson.slaves.SlaveComputer;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HostPathVolumeSource;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;

/**
 * Launches on Kubernetes the specified {@link KubernetesComputer} instance.
 */
public class KubernetesLauncher extends JNLPLauncher {

    @CheckForNull
    private transient AllContainersRunningPodWatcher watcher;

    private static final Logger LOGGER = Logger.getLogger(KubernetesLauncher.class.getName());

    static final String JOB_NAME_LABEL = "job-name";
    private static final String DEFAULT_STORAGE_CLASS = "openebs-standalone";
    private static final String OPENEBS_IO_TARGET_AFFINITY = "openebs.io/target-affinity";
    private static final String DEFAULT_ACCESS_MODE = "ReadWriteOnce";
    private static final String STORAGE = "storage";
    private static final int DEFAULT_HOME_VOLUME_SIZE_GB = 1;

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
            Pod pod = template.build(client, slave);

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

                String buildableName = buildable.task.getDisplayName();
                //set the label string early, so that Jenkins can provision for other jobs earlier
                slave.setLabelString(buildableName);

                // adjust pod dynamically to the job that gets scheduled to it
                String hashedVolumeName = adjustPodToBuildable(pod, buildable, namespace);

                // provision dynamic pvc home volume in kubernetes
                claimHomeVolume(hashedVolumeName,
                   buildableName,
                   namespace,
                   DEFAULT_HOME_VOLUME_SIZE_GB,
                   client);

                LOGGER.log(Level.FINE, "Creating Pod: {0} in namespace {1}", new Object[]{podId, namespace});
                pod = client.pods().inNamespace(namespace).create(pod);
                LOGGER.log(INFO, "Created Pod: {0} in namespace {1}", new Object[]{podId, namespace});
                listener.getLogger().printf("Created Pod: %s in namespace %s%n", podId, namespace);
            }
            String podName = pod.getMetadata().getName();
            String namespace1 = pod.getMetadata().getNamespace();
            watcher = new AllContainersRunningPodWatcher(client, pod);
            try (Watch _w = client.pods().inNamespace(namespace1).withName(podName).watch(watcher)){
                watcher.await(template.getSlaveConnectTimeout(), TimeUnit.SECONDS);
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

    BuildableItem findFirstBuildableToProvisionFor(List<BuildableItem> buildables, String namespace,
            String templateLabel, KubernetesClient client) {

        Set<LabelAtom> templateLabelSet = Label.parse(templateLabel);

        for (BuildableItem buildable : buildables) {
            String name = buildable.task.getName();
            LOGGER.log(Level.FINE, "checking if buildable {0} should be provisioned", name);

            if (buildable instanceof NotWaitingItem) {
                //this item is not waiting
                LOGGER.log(Level.FINEST, "buildable {0} is not waiting", name);
                if (KubernetesCloud.filterActiveAgentPods(client.pods()
                        .inNamespace(namespace)
                        .withLabel(JOB_NAME_LABEL, name)
                        .list())
                        .isEmpty()) {
                    LOGGER.log(Level.FINEST, "buildable {0} has no other active pod running", name);
                    //no other active pod exists for this job
                    Label labelExpression = parseLabelExpression(buildable.task.getAssignedLabel().getExpression());
                    if (labelExpression != null && labelExpression.matches(templateLabelSet)) {
                        //the template label satisfies the job's label expression, provision
                        LOGGER.log(Level.FINE, "buildable {0} has all criteria true, provision!", name);
                        return buildable;
                    }
                }
            }
        }
        return null;
    }

    String adjustPodToBuildable(Pod pod, BuildableItem buildable, String namespace) {
        String buildableName = buildable.task.getDisplayName();
        String fullBuildableName = buildable.task.getUrl().replace("job/", "");
        LOGGER.log(INFO, "buildableName : {0}, fullBuildableName: {1}", new Object[] {buildableName, fullBuildableName});

        // label pod with job name
        pod.getMetadata().getLabels().put(JOB_NAME_LABEL, buildableName);

        Container jnlp = findJnlpContainer(pod.getSpec().getContainers());
        if (jnlp != null) {
            // add full job name as environment variable for script inside the agent container
            jnlp.getEnv().add(new EnvVarBuilder().withName("FULL_JOB_NAME").withValue(fullBuildableName).build());
        } else {
            LOGGER.log(WARNING, "could not find jnlp container volume to adjust : {0}", pod);
        }

        // calculate a hashed name from the job name. Using names like "enterprise-platform-master-snapshot" for the PVC
        // leads to errors in PVC creation, because controller names that are generated from such are long
        // PVC name are too long for kubernetes' 63 character limit
        String hashedVolumeName = calcHashedVolumeName("job", buildableName);

        // adjust the workspace directory on the host to contain a directory for the namespace and the hashedVolumeName
        adjustWorkspaceVolume(pod, hashedVolumeName, namespace);

        // adjust the name of the PVC reference for the home volume (actual claim happens later)
        adjustHomeClaim(pod, hashedVolumeName);

        return hashedVolumeName;
    }

    public static String calcHashedVolumeName(String prefix, String buildableName) {
        return prefix + "-" + Integer.toHexString(buildableName.hashCode());
    }

    void adjustWorkspaceVolume(Pod pod, String hashedVolumeName, String namespace) {
        // find the HostPath workspace volume
        HostPathVolumeSource hostPath = findWorkspaceVolumeSource(pod);
        if (hostPath != null) {
            // adjust HostPath workspace volume for specific job
            hostPath.setPath(hostPath.getPath() + "/" + namespace + "/" + hashedVolumeName);
            LOGGER.log(FINE, "adjusted hostpath volume : {0}", hostPath);
        } else {
            LOGGER.log(WARNING, "could not find hostpath volume to adjust : {0}", hostPath);
        }
    }
    
    void adjustHomeClaim(Pod pod, String pvcName) {
        // adjust pvc name
        Volume homeVolume = findHomeVolume(pod.getSpec().getVolumes());
        if (homeVolume != null) {
            homeVolume.getPersistentVolumeClaim().setClaimName(pvcName);
            LOGGER.log(FINE, "adjusted pvc : {0}", pvcName);
        } else {
            LOGGER.log(WARNING, "could not find pvc volume to adjust : {0}", pvcName);
        }

        // add pvc name as openebs target affinity to agent pod so that it and the openebs controller pod
        // get scheduled on the same node
        pod.getMetadata().getLabels().put(OPENEBS_IO_TARGET_AFFINITY, pvcName);
    }

    Container findJnlpContainer(List<Container> containers) {
        for (Container c : containers) {
            if (c.getName().equals(KubernetesCloud.JNLP_NAME)) {
                return c;
            }
        }
        return null;
    }

    Volume findHomeVolume(List<Volume> volumes) {
        for (Volume v : volumes) {
            if (v.getName().equals(PodTemplateBuilder.HOME_VOLUME_NAME)) {
                return v;
            }
        }
        return null;
    }

    HostPathVolumeSource findWorkspaceVolumeSource(Pod pod) {
        List<Volume> volumes = pod.getSpec().getVolumes();
        for (Volume vol : volumes) {
            HostPathVolumeSource hostPath = vol.getHostPath();
            if (hostPath != null) {
                LOGGER.log(FINE, "found job host path: {0}", hostPath);
                return hostPath;
            }
        }
        return null;
    }

    void claimHomeVolume(String pvcName, String buildableName, String namespace, int sizeGB,
        KubernetesClient client) throws IOException {

        LOGGER.log(FINE, "checking volume in kubernetes: {0}", pvcName);

        List<PersistentVolumeClaim> claims = client.persistentVolumeClaims()
            .inNamespace(namespace)
            .withLabel(JOB_NAME_LABEL, buildableName)
            .list()
            .getItems();

        if (!claims.isEmpty()) {
            // volume already exists, nothing to do
            LOGGER.log(FINE, "volume claim already exists in kubernetes, skipping: {0}", pvcName);
            return;
        }

        LOGGER.log(FINE, "creating volume claim in kubernetes: {0}", pvcName);

        Map<String, Quantity> reqMap = new HashMap<>();
        reqMap.put(STORAGE, new Quantity(sizeGB + "Gi"));

        Map<String, String> labels = new HashMap<>();
        labels.put(JOB_NAME_LABEL, buildableName);
        labels.put(OPENEBS_IO_TARGET_AFFINITY, pvcName);

        client.persistentVolumeClaims()
            .inNamespace(namespace)
            .createOrReplaceWithNew()
            .withNewMetadata()
            .withName(pvcName)
            .withLabels(labels)
            .endMetadata()
            .withNewSpec()
            .withAccessModes(DEFAULT_ACCESS_MODE)
            .withNewResources()
            .withRequests(reqMap)
            .endResources()
            .withStorageClassName(DEFAULT_STORAGE_CLASS)
            .endSpec()
            .done();

        LOGGER.log(Level.FINE, "claimed home pvc: {0}", pvcName);
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
}
