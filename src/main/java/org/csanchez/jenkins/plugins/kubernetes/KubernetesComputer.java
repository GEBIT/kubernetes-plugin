package org.csanchez.jenkins.plugins.kubernetes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.acegisecurity.Authentication;
import org.apache.commons.lang.StringUtils;
import org.jenkinsci.plugins.cloudstats.ProvisioningActivity.Id;
import org.jenkinsci.plugins.cloudstats.TrackedItem;
import org.jenkinsci.plugins.kubernetes.auth.KubernetesAuthException;
import org.jenkinsci.plugins.workflow.support.steps.ExecutorStepExecution;
import org.kohsuke.stapler.QueryParameter;
import org.kohsuke.stapler.StaplerRequest;
import org.kohsuke.stapler.StaplerResponse;
import org.kohsuke.stapler.export.Exported;
import org.kohsuke.stapler.framework.io.ByteBuffer;
import org.kohsuke.stapler.framework.io.LargeText;

import hudson.model.Computer;
import hudson.model.Executor;
import hudson.model.Item;
import hudson.model.Queue;
import hudson.security.ACL;
import hudson.security.Permission;
import hudson.slaves.AbstractCloudComputer;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.EventList;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import jenkins.model.Jenkins;
import jenkins.security.MasterToSlaveCallable;

/**
 * @author Carlos Sanchez carlos@apache.org
 */
public class KubernetesComputer extends AbstractCloudComputer<KubernetesSlave> implements TrackedItem {

    private static final Logger LOGGER = Logger.getLogger(KubernetesComputer.class.getName());

    //VisibleForTesting
    static final String GEBIT_BUILD_CLUSTER_WORKSPACE_PATH = "GEBIT_BUILD_CLUSTER_WORKSPACE_PATH";

    // id for cloud-stats plugin
    private transient Id id;

    public KubernetesComputer(KubernetesSlave slave) {
        super(slave);
        id = slave.getId();
    }

    @Override
    public void taskAccepted(Executor executor, Queue.Task task) {
        super.taskAccepted(executor, task);
        Queue.Executable exec = executor.getCurrentExecutable();
        runRsync(task, Rsync.Direction.PULL);
        LOGGER.log(Level.FINE, " Computer {0} accepted task {1}", new Object[] {this, exec});
    }

    @Override
    public void taskCompleted(Executor executor, Queue.Task task, long durationMS) {
        Queue.Executable exec = executor.getCurrentExecutable();
        runRsync(task, Rsync.Direction.PUSH);
        LOGGER.log(Level.FINE, " Computer {0} completed task {1}", new Object[] {this, exec});

        // May take the agent offline and remove it, in which case getNode()
        // above would return null and we'd not find our DockerSlave anymore.
        super.taskCompleted(executor, task, durationMS);
    }

    @Override
    public void taskCompletedWithProblems(Executor executor, Queue.Task task, long durationMS, Throwable problems) {
        super.taskCompletedWithProblems(executor, task, durationMS, problems);
        Queue.Executable exec = executor.getCurrentExecutable();
        runRsync(task, Rsync.Direction.PUSH);
        LOGGER.log(Level.FINE, " Computer {0} completed task {1} with problems", new Object[] {this, exec});
    }

    @Exported
    public List<Container> getContainers() throws KubernetesAuthException, IOException {
        if(!Jenkins.get().hasPermission(Jenkins.ADMINISTER)) {
            LOGGER.log(Level.FINE, " Computer {0} getContainers, lack of admin permission, returning empty list", this);
            return Collections.emptyList();
        }

        KubernetesSlave slave = getNode();
        if(slave == null) {
            return Collections.emptyList();
        }

        KubernetesCloud cloud = slave.getKubernetesCloud();
        KubernetesClient client = cloud.connect();

        String namespace = StringUtils.defaultIfBlank(slave.getNamespace(), client.getNamespace());
        Pod pod = client.pods().inNamespace(namespace).withName(getName()).get();

        return pod.getSpec().getContainers();
    }

    @Exported
    public List<Event> getPodEvents() throws KubernetesAuthException, IOException {
        if(!Jenkins.get().hasPermission(Jenkins.ADMINISTER)) {
            LOGGER.log(Level.FINE, " Computer {0} getPodEvents, lack of admin permission, returning empty list", this);
            return Collections.emptyList();
        }

        KubernetesSlave slave = getNode();
        if(slave != null) {
            KubernetesCloud cloud = slave.getKubernetesCloud();
            KubernetesClient client = cloud.connect();

            String namespace = StringUtils.defaultIfBlank(slave.getNamespace(), client.getNamespace());

            Pod pod = client.pods().inNamespace(namespace).withName(getName()).get();
            if(pod != null) {
                ObjectMeta podMeta = pod.getMetadata();
                String podNamespace = podMeta.getNamespace();

                Map<String, String> fields = new HashMap<>();
                fields.put("involvedObject.uid", podMeta.getUid());
                fields.put("involvedObject.name", podMeta.getName());
                fields.put("involvedObject.namespace", podNamespace);

                EventList eventList = client.events().inNamespace(podNamespace).withFields(fields).list();
                if(eventList != null) {
                    return eventList.getItems();
                }
            }
        }

        return Collections.emptyList();
    }

    public void doContainerLog(@QueryParameter String containerId,
                               StaplerRequest req, StaplerResponse rsp) throws KubernetesAuthException, IOException {
        Jenkins.get().checkPermission(Jenkins.ADMINISTER);

        ByteBuffer outputStream = new ByteBuffer();
        KubernetesSlave slave = getNode();
        if(slave != null) {
            KubernetesCloud cloud = slave.getKubernetesCloud();
            KubernetesClient client = cloud.connect();

            String namespace = StringUtils.defaultIfBlank(slave.getNamespace(), client.getNamespace());

            client.pods().inNamespace(namespace).withName(getName())
                    .inContainer(containerId).tailingLines(20).watchLog(outputStream);
        }

        new LargeText(outputStream, false).doProgressText(req, rsp);
    }

    @Override
    public String toString() {
        return String.format("KubernetesComputer name: %s slave: %s", getName(), getNode());
    }

    @Override
    public ACL getACL() {
        final ACL base = super.getACL();
        return new ACL() {
            @Override
            public boolean hasPermission(Authentication a, Permission permission) {
                return permission == Computer.CONFIGURE ? false : base.hasPermission(a,permission);
            }
        };
    }

    public Id getId() {
        return id;
    }

    public void setId(Id id) {
        this.id = id;
    }

    private boolean isPipelineJob(Queue.Task task) {
        return task instanceof ExecutorStepExecution.PlaceholderTask;
    }

    private boolean isMavenModuleSet(Queue.Task task) {
        return task.getClass().getSimpleName().equals("MavenModuleSet");
    }

    private boolean isMatrixJob(Queue.Task task) {
        return task.getClass().getSimpleName().equals("MatrixConfiguration");
    }

    public String calcWorkspacePath(Queue.Task task) {
        String workspacePath = null;
        if (isMavenModuleSet(task)){
            workspacePath = ((Item) task).getFullName();
        } else if (isMatrixJob(task)) {
            workspacePath = ((Item) task).getFullName().replace("=", "/");
        // we never rsync automatically if it's a pipeline job (has to be done explicitly by
        // the pipeline author)
        //} else if (isPipelineJob(task)) {
        //    workspacePath = ((Item) task.getOwnerTask()).getFullName();
        } else {
            LOGGER.log(Level.SEVERE, "unable to calculate workspacePath for task: {0}", new Object[]{task});
            throw new IllegalStateException("unable to calculate workspacePath for task: " + task);
        }

        LOGGER.log(Level.INFO, "Calculated workspacePath: {0} for task: {1}", new Object[]{workspacePath, task});
        return workspacePath;
    }

    private void runRsync(Queue.Task task, Rsync.Direction direction) {
        if (!isPipelineJob(task)) {
            try {
                LOGGER.log(Level.INFO, "running rsync in direction {0} for Computer {1} and task {2}", new Object[] {direction, getName(), task});
                RsyncResult rsyncResult = getChannel().call(new Rsync(calcWorkspacePath(task), direction));
                LOGGER.log(Level.INFO, "rsync finished with exit code {0} for direction {1} for Computer {2} and task {3}\noutput:\n{4}",
                        new Object[] {rsyncResult.exitCode, direction, getName(), task, rsyncResult.output});
                if (rsyncResult.exitCode != 0) {
                    throw new IOException("rsync returned non-zero exit code: " + rsyncResult.exitCode + "\noutput:\n" + rsyncResult.output);
                }
            } catch (IOException | RuntimeException | InterruptedException e) {
                LOGGER.log(Level.SEVERE, "Exception in runRsync: {0}", e);
            }
        }
    }

    static class RsyncResult implements Serializable {

        private static final long serialVersionUID = 1L;

        public int exitCode;
        public String output;

        RsyncResult(int exitCode, String output) {
            this.exitCode = exitCode;
            this.output = output;
        }
    }

    static class Rsync extends MasterToSlaveCallable<RsyncResult,IOException> {

        private static final long serialVersionUID = 1L;

        static enum Direction { PULL, PUSH };

        protected String workspacePath;
        protected String rsyncCmd;

        Rsync(String workspacePath, Direction direction) {
            this.workspacePath = workspacePath;
            this.rsyncCmd = "/usr/local/bin/" + direction.toString().toLowerCase() + "-workspace.sh";
        }

        public RsyncResult call() throws IOException {
            try {
                ProcessBuilder pb = new ProcessBuilder(rsyncCmd);
                pb.redirectErrorStream(true);
                Map<String, String> env = pb.environment();
                env.put(GEBIT_BUILD_CLUSTER_WORKSPACE_PATH, workspacePath);
                Process rsyncProc = pb.start();
                BufferedReader in = new BufferedReader(new InputStreamReader(rsyncProc.getInputStream()));
                String line;
                StringBuffer sb = new StringBuffer();
                while ((line = in.readLine()) != null) {
                    sb.append(line + "\n");
                }
                int exitCode = rsyncProc.waitFor();
                return new RsyncResult(exitCode, sb.toString());
            } catch (InterruptedException e) {
                return new RsyncResult(-1, e.toString());
            }
        }
    }
}

