package org.csanchez.jenkins.plugins.kubernetes;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jenkinsci.plugins.kubernetes.auth.KubernetesAuthException;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeCondition;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.client.dsl.Resource;

/**
 * This class is responsible for providing information about pending launches of agent pods
 * for the whole cluster. It creates a shared namespace for all jenkins instances where it
 * manages the number of pending launches globally.
 * 
 * It also provides utility methods for calculating available and used resources.
 * 
 * @author dschiemann
 *
 */
public class KubernetesCloudLimiter {

    private static final Logger LOGGER = Logger.getLogger(KubernetesCloudLimiter.class.getName());

    // package-private for testing
    static final String COMPUTE_LABEL_VALUE = "general";
    static final String COMPUTE_LABEL_NAME = "compute";
    static final String GLOBAL_CONFIG_MAP_NAME = "default";
    static final String LOCKED = "locked";
    static final String GLOBAL_NAMESPACE = "jenkins-masters";
    static final String PENDING_CPU_MILLIS = "pendingCpuMillis";
    static final String PENDING_MEM_MI = "pendingMemMi";
    static final long MAX_ĹOCK_TIME_MS = 60 * 1000;
    static final int MAX_TRIES = 5;

    private transient KubernetesCloud cloud;

    public KubernetesCloudLimiter(KubernetesCloud cloud) {
        this.cloud = cloud;
    }

    private Resource<ConfigMap, DoneableConfigMap> findGlobalConfigMap() throws KubernetesAuthException, IOException {

        return cloud.connect().configMaps().inNamespace(GLOBAL_NAMESPACE).withName(GLOBAL_CONFIG_MAP_NAME);
    }

    private ConfigMap getGlobalConfigMap() throws KubernetesAuthException, IOException {

        return findGlobalConfigMap().get();
    }

    void checkGlobalConfigMap() throws KubernetesAuthException, IOException {
        ConfigMap globalConfigMap = getGlobalConfigMap();

        if (globalConfigMap == null) {
            // ensure global namespace exists
            cloud.connect().namespaces().createOrReplaceWithNew().
                withNewMetadata().
                    withName(GLOBAL_NAMESPACE).
                endMetadata().
                done();

            //init new data map
            Map<String, String> data = new HashMap<>();
            data.put(PENDING_CPU_MILLIS, "0");
            data.put(PENDING_MEM_MI, "0");
            data.put(LOCKED, "false");

            cloud.connect().configMaps().inNamespace(GLOBAL_NAMESPACE).createOrReplaceWithNew().
                withNewMetadata().
                  withName(GLOBAL_CONFIG_MAP_NAME).
                endMetadata().
                withData(data).
              done();
        }
    }

    public boolean acquireLock() throws KubernetesAuthException, IOException, InterruptedException {
        return acquireLock(false);
    }

    private boolean acquireLock(boolean force) throws KubernetesAuthException, IOException, InterruptedException {
        checkGlobalConfigMap();

        int numTries = 0;
        boolean locked = true;
        while (numTries < MAX_TRIES) {
            //get the global config map
            ConfigMap cm = getGlobalConfigMap();
            if (cm == null) {
                LOGGER.log(Level.SEVERE, "global config map does not exist");
                return false;
            }
            locked = Boolean.parseBoolean(cm.getData().get(LOCKED));
            if (locked && !force) {
                //wait a second, then try again
                Thread.sleep(1000);
            } else {
                //we lock for us now
                findGlobalConfigMap().edit().addToData(LOCKED, "true").done();
                return true;
            }
            numTries++;
        }
        if (locked) {
            // if its still locked after MAX_RETRIES, force it
            LOGGER.log(Level.SEVERE, "forcing lock for global config map");
            return acquireLock(true);
        }
        LOGGER.log(Level.SEVERE, "number of max tries reached to acquire lock for global config map");
        return false;
    }

    int getPendingCpuMillis() throws KubernetesAuthException, IOException {
        checkGlobalConfigMap();

        ConfigMap cm = getGlobalConfigMap();
        return Integer.parseInt(cm.getData().get(PENDING_CPU_MILLIS));
    }

    int getPendingMemMi() throws KubernetesAuthException, IOException {
        checkGlobalConfigMap();

        ConfigMap cm = getGlobalConfigMap();
        return Integer.parseInt(cm.getData().get(PENDING_MEM_MI));
    }

    public void releaseLock() throws KubernetesAuthException, IOException {
        checkGlobalConfigMap();

        findGlobalConfigMap().edit().addToData(LOCKED, "false").done();
    }

    public void incPending(PodTemplate template) throws KubernetesAuthException, IOException {
        // build the pod now and estimate it, so the yaml from the template is taken into account too
        Pod pod = new PodTemplateBuilder(template).build();

        int pendingCpuMillis = getPendingCpuMillis();
        int requestedCpuMillis = getPodCpuRequestMillis(pod);
        int totalPendingCpuMillis = pendingCpuMillis + requestedCpuMillis;

        findGlobalConfigMap().edit().addToData(PENDING_CPU_MILLIS, Integer.toString(totalPendingCpuMillis)).done();
        LOGGER.log(Level.FINE, "inc PENDING_CPU_MILLIS to: {0}", totalPendingCpuMillis);

        int pendingMemMillis = getPendingMemMi();
        int requestedMemMillis = getPodMemRequestMi(pod);
        int totalPendingMemMillis = pendingMemMillis + requestedMemMillis;

        findGlobalConfigMap().edit().addToData(PENDING_MEM_MI, Integer.toString(totalPendingMemMillis)).done();
        LOGGER.log(Level.FINE, "inc PENDING_MEM_MI to: {0}", totalPendingMemMillis);
    }

    public void decPending(PodTemplate template) throws KubernetesAuthException, IOException {
        // build the pod now and estimate it, so the yaml from the template is taken into account too
        Pod pod = new PodTemplateBuilder(template).build();

        int pendingCpuMillis = getPendingCpuMillis();
        int provisionedCpuMillis = getPodCpuRequestMillis(pod);
        int totalPendingCpuMillis = pendingCpuMillis - provisionedCpuMillis;

        findGlobalConfigMap().edit().addToData(PENDING_CPU_MILLIS, Integer.toString(totalPendingCpuMillis)).done();
        LOGGER.log(Level.FINE, "dec PENDING_CPU_MILLIS to: {0}", totalPendingCpuMillis);

        int pendingMemMillis = getPendingMemMi();
        int provisionedMemMillis = getPodMemRequestMi(pod);
        int totalPendingMemMillis = pendingMemMillis - provisionedMemMillis;

        findGlobalConfigMap().edit().addToData(PENDING_MEM_MI, Integer.toString(totalPendingMemMillis)).done();
        LOGGER.log(Level.FINE, "dec PENDING_MEM_MI to: {0}", totalPendingMemMillis);
    }

    private boolean isNodeReady(Node node) {
        //first, check unschedulable bit (true, if the node was cordoned)
        Boolean unschedulable = node.getSpec().getUnschedulable();
        if (unschedulable != null && unschedulable) {
            return false;
        }

        //node is not cordoned, check all stati for readiness
        for (NodeCondition condition : node.getStatus().getConditions()) {
            if ("Ready".equals(condition.getType())) {
                return Boolean.parseBoolean(condition.getStatus());
            }
        }
        return false;
    }

    public int estimateNumSchedulablePods(PodTemplate template) throws KubernetesAuthException, IOException {
        LOGGER.log(Level.FINE, "estimating for template: {0}", template);

        // build the pod now and estimate it, so the yaml from the template is taken into account too
        Pod pod = new PodTemplateBuilder(template).build();

        return Math.max(0, Math.min(estimateByCpu(pod), estimateByMem(pod)));
    }

    public int estimateByCpu(Pod pod) throws KubernetesAuthException, IOException {
        // how much the template requests
        int cpuRequest = getPodCpuRequestMillis(pod);
        if (cpuRequest == 0) {
            // we do not allow templates with an undefined cpu request value
            // templates MUST define a cpu request
            LOGGER.log(Level.WARNING, "PodTemplate {0} has 0 CPU request, will not be scheduled", pod.getMetadata().getName());
            return 0;
        }
        // how much is allocatable (maximum capacity) of the cluster
        int allocatableCpu = getAllocatableCpuMillis();
        // how much is used over the whole cluster
        int usedCpu = getUsedCpuMillis();
        // the currently pending cpu millis
        int pendingCpu = getPendingCpuMillis();
        // how much is available for the whole cluster
        int availableCpu = allocatableCpu - usedCpu - pendingCpu;

        return (availableCpu / cpuRequest);
    }

    public int estimateByMem(Pod pod) throws KubernetesAuthException, IOException {
        // how much the template requests
        int memRequest = getPodMemRequestMi(pod);
        if (memRequest == 0) {
            // we do not allow templates with an undefined mem request value
            // templates MUST define a mem request
            LOGGER.log(Level.WARNING, "Pod {0} has 0 MEM request, will not be scheduled", pod.getMetadata().getName());
            return 0;
        }
        // how much is allocatable (maximum capacity) of the cluster
        int allocatableMem = getAllocatableMemMi();
        // how much is used over the whole cluster
        int usedMem = getUsedMemMi();
        // the currently pending mem millis
        int pendingMem = getPendingMemMi();
        // how much is available for the whole cluster
        int availableMem = allocatableMem - usedMem - pendingMem;

        return (availableMem / memRequest);
    }

    int getPodCpuRequestMillis(Pod pod) {
        int podCpuRequestMillis = 0;

        for (Container container : pod.getSpec().getContainers()) {
            ResourceRequirements resources = container.getResources();
            if (resources != null) {
                Map<String, Quantity> requests = resources.getRequests();
                if (requests != null) {
                    Quantity cpu = requests.get("cpu");
                    if (cpu != null) {
                        podCpuRequestMillis += calcCpuQuantityMillis(cpu);
                    }
                }
            }
        }

        return podCpuRequestMillis;
    }

    int getPodMemRequestMi(Pod pod) {
        int podMemRequestMi = 0;

        for (Container container : pod.getSpec().getContainers()) {
            ResourceRequirements resources = container.getResources();
            if (resources != null) {
                Map<String, Quantity> requests = resources.getRequests();
                if (requests != null) {
                    Quantity mem = requests.get("memory");
                    if (mem != null) {
                        podMemRequestMi += calcMemQuantityMi(mem);
                    }
                }
            }
        }

        return podMemRequestMi;
    }

    int getAllocatableCpuMillis() throws KubernetesAuthException, IOException {
        int allocatableCpuMillis = 0;

        List<Node> nodes = cloud.connect().nodes().withLabel(COMPUTE_LABEL_NAME, COMPUTE_LABEL_VALUE).list().getItems();

        for (Node node : nodes) {
            if (isNodeReady(node)) {
                Quantity cpuAlloc = node.getStatus().getAllocatable().get("cpu");
                LOGGER.log(Level.FINE, "node {0} has cpu allocatable {1}", new Object[] {node.getMetadata().getName(), cpuAlloc});
                allocatableCpuMillis += calcCpuQuantityMillis(cpuAlloc);
            }
        }

        return allocatableCpuMillis;
    }

    int getAllocatableMemMi() throws KubernetesAuthException, IOException {
        int allocatableMemMi = 0;

        List<Node> nodes = cloud.connect().nodes().withLabel(COMPUTE_LABEL_NAME, COMPUTE_LABEL_VALUE).list().getItems();

        for (Node node : nodes) {
            if (isNodeReady(node)) {
                Quantity memAlloc = node.getStatus().getAllocatable().get("memory");
                LOGGER.log(Level.FINE, "node {0} has mem allocatable {1}", new Object[] {node.getMetadata().getName(), memAlloc});
                allocatableMemMi += calcMemQuantityMi(memAlloc);
            }
        }

        return allocatableMemMi;
    }

    int getUsedCpuMillis() throws KubernetesAuthException, IOException {
        int usedCpuMillis = 0;

        List<Pod> activeAgentPods = KubernetesCloud.filterRunningOrPendingAgentPods(cloud.connect().pods().inAnyNamespace().withLabel("jenkins", "slave").list());
        LOGGER.log(Level.FINE, "number of active agent pods: {0}", activeAgentPods.size());

        for (Pod agentPod : activeAgentPods) {
            int podUsedCpuMillis = 0;

            List<Container> containers = agentPod.getSpec().getContainers();
            for (Container container : containers) {
                Map<String, Quantity> requests = container.getResources().getRequests();
                if (requests != null) {
                    Quantity cpuQuantity = requests.get("cpu");
                    if (cpuQuantity != null) {
                        podUsedCpuMillis += calcCpuQuantityMillis(cpuQuantity);
                    }
                }
                LOGGER.log(Level.FINE, "container {0} in pod {1} has cpu requests {2} ", new Object[] {container.getName(), agentPod.getMetadata().getName(), requests});
            }
            usedCpuMillis += podUsedCpuMillis;
        }

        return usedCpuMillis;
    }

    int getUsedMemMi() throws KubernetesAuthException, IOException {
        int usedMemMi = 0;

        List<Pod> activeAgentPods = KubernetesCloud.filterRunningOrPendingAgentPods(cloud.connect().pods().inAnyNamespace().withLabel("jenkins", "slave").list());
        LOGGER.log(Level.FINE, "number of active agent pods: {0}", activeAgentPods.size());

        for (Pod agentPod : activeAgentPods) {
            int podUsedMemMi = 0;

            List<Container> containers = agentPod.getSpec().getContainers();
            for (Container container : containers) {
                Map<String, Quantity> requests = container.getResources().getRequests();
                if (requests != null) {
                    Quantity memQuantity = requests.get("memory");
                    if (memQuantity != null) {
                        podUsedMemMi += calcMemQuantityMi(memQuantity);
                    }
                }
                LOGGER.log(Level.FINE, "container {0} in pod {1} has mem requests {2} ", new Object[] {container.getName(), agentPod.getMetadata().getName(), requests});
            }
            usedMemMi += podUsedMemMi;
        }

        return usedMemMi;
    }

    private int calcCpuQuantityMillis(Quantity quantity) {
        int quantityMillis = 0;
        String format = quantity.getFormat();
        String amount = quantity.getAmount();

        try {
            if (amount.length() == 0) {
                return 0;
            } else if (format.equals("m")) {
                quantityMillis = Integer.parseInt(amount);
            } else if (amount.contains(".")) {
                quantityMillis = (int) (Double.parseDouble(amount) * 1000);
            } else {
                quantityMillis = Integer.parseInt(amount) * 1000;
            }
        } catch (NumberFormatException e) {
            LOGGER.log(Level.WARNING, "Failed to parse CPU quantity {0} as cpu millis", quantity);
        }

        return quantityMillis;
    }

    long longPow(int a, int b) {
        return (long) Math.pow(a, b);
    }

    private long calcMemQuantityMi(Quantity quantity) {
        long quantityMi = 0;
        String format = quantity.getFormat();
        String amount = quantity.getAmount();

        try {
            if (amount.length() == 0) {
                return 0;
            } else if (format.equals("K")) {
                quantityMi = Long.parseLong(amount) * 1000 / longPow(2, 20);
            } else if (format.equals("M")) {
                quantityMi = Long.parseLong(amount) * longPow(1000, 2) / longPow(2, 20);
            } else if (format.equals("G")) {
                quantityMi = Long.parseLong(amount) * longPow(1000, 3) / longPow(2, 20);
            } else if (format.equals("T")) {
                quantityMi = Long.parseLong(amount) * longPow(1000, 4) / longPow(2, 20);
            } else if (format.equals("P")) {
                quantityMi = Long.parseLong(amount) * longPow(1000, 5) / longPow(2, 20);
            } else if (format.equals("E")) {
                quantityMi = Long.parseLong(amount) * longPow(1000, 6) / longPow(2, 20);
            } else if (format.equals("Ki")) {
                quantityMi = Long.parseLong(amount) / 1024;
            } else if (format.equals("Mi")) {
                quantityMi = Long.parseLong(amount);
            } else if (format.equals("Gi")) {
                quantityMi = Long.parseLong(amount) * 1024;
            } else if (format.equals("Ti")) {
                quantityMi = Long.parseLong(amount) * longPow(1024, 2);
            } else if (format.equals("Pi")) {
                quantityMi = Long.parseLong(amount) * longPow(1024, 3);
            } else if (format.equals("Ei")) {
                quantityMi = Long.parseLong(amount) * longPow(1024, 4);
            } else {
                //bytes
                quantityMi = Long.parseLong(amount) / 1024 / 1024;
            }
        } catch (NumberFormatException e) {
            LOGGER.log(Level.WARNING, "Failed to parse MEM quantity {0} as mem Mi", quantity);
        }

        return quantityMi;
    }
}
