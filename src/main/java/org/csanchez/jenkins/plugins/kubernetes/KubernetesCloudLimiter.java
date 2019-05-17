package org.csanchez.jenkins.plugins.kubernetes;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import hudson.model.Label;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeCondition;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
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
    static final String NUM_PENDING_LAUNCHES = "numPendingLaunches";
    static final long MAX_ĹOCK_TIME_MS = 60 * 1000;
    static final int MAX_TRIES = 5;

    private transient KubernetesCloud cloud;

    public KubernetesCloudLimiter(KubernetesCloud cloud) {
        this.cloud = cloud;
    }

    private Resource<ConfigMap, DoneableConfigMap> findGlobalConfigMap() throws UnrecoverableKeyException,
        NoSuchAlgorithmException, KeyStoreException, IOException, CertificateEncodingException {

        return cloud.connect().configMaps().inNamespace(GLOBAL_NAMESPACE).withName(GLOBAL_CONFIG_MAP_NAME);
    }

    private ConfigMap getGlobalConfigMap() throws UnrecoverableKeyException, NoSuchAlgorithmException,
        KeyStoreException, IOException, CertificateEncodingException {

        return findGlobalConfigMap().get();
    }

    private void checkGlobalConfigMap() throws UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, IOException {
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
            data.put(NUM_PENDING_LAUNCHES, "0");
            data.put(LOCKED, "false");

            cloud.connect().configMaps().inNamespace(GLOBAL_NAMESPACE).createOrReplaceWithNew().
                withNewMetadata().
                  withName(GLOBAL_CONFIG_MAP_NAME).
                endMetadata().
                withData(data).
              done();
        }
    }

    public boolean acquireLock() throws InterruptedException, UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, IOException {
        return acquireLock(false);
    }

    private boolean acquireLock(boolean force) throws InterruptedException, UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, IOException {
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
            return acquireLock(true);
        }
        LOGGER.log(Level.SEVERE, "number of max tries reached to acquire lock for global config map");
        return false;
    }

    public void releaseLock() throws UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, IOException {
        checkGlobalConfigMap();

        findGlobalConfigMap().edit().addToData(LOCKED, "false").done();
    }

    public int getNumOfPendingLaunchesK8S() throws UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, IOException {
        checkGlobalConfigMap();

        ConfigMap cm = getGlobalConfigMap();
        String numPendingLaunches = cm.getData().get(NUM_PENDING_LAUNCHES);
        if (numPendingLaunches != null) {
            LOGGER.log(Level.FINE, "get numPendingLaunches: {0}", numPendingLaunches);
            return Integer.parseInt(numPendingLaunches);
        } else {
            return 0;
        }
    }

    public void setNumOfPendingLaunchesK8S(int numPendingLaunches) throws UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, IOException {
        checkGlobalConfigMap();

        // while developing, a jenkins-master can get "force undeployed" before all of it's provisionings and jobs are
        // finished. This can lead to numPendingLaunches of less than 0. Guard against this.
        if (numPendingLaunches < 0) {
            numPendingLaunches = 0;
        }
        findGlobalConfigMap().edit().addToData(NUM_PENDING_LAUNCHES, Integer.toString(numPendingLaunches)).done();
        LOGGER.log(Level.FINE, "set numPendingLaunches to: {0}", numPendingLaunches);
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

    public int getNumSchedulablePods(Label label) throws UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, IOException {
        // how much the template requests 
        int cpuRequest = getPodTemplateCpuRequestMillis(label);
        // how much is allocatable (maximum capacity) per node
        Map<String, Integer> allocatableCpu = getAllocatableCpuMillisByNode();
        // how much is used per node
        Map<String, Integer> usedCpu = getUsedCpuMillisByNode();
        // how much is available per node (difference of allocatable and used)
        Map<String, Integer> availableCpu = calcAvailableCpuMillis(allocatableCpu, usedCpu);

        int result = 0;
        for (Integer availableCpuOnOneNode : availableCpu.values()) {
            // count the number of times the requests would fit on each node
            result += availableCpuOnOneNode / cpuRequest;
        }
        return result;
    }

    int getPodTemplateCpuRequestMillis(Label label) {
        int podTemplateCpuRequestMillis = 0;

        PodTemplate podTemplate = cloud.getTemplate(label);

        for (ContainerTemplate containerTemplate : podTemplate.getContainers()) {
            podTemplateCpuRequestMillis += calcQuantityMillis(containerTemplate.getResourceRequestCpu());
        }

        return podTemplateCpuRequestMillis;
    }

    private Map<String, Integer> calcAvailableCpuMillis(Map<String, Integer> allocatable, Map<String, Integer> used) {
        Map<String, Integer> result = new HashMap<>();

        // calculate the difference of allocatable to used for each node
        for (String nodeName : allocatable.keySet()) {
            Integer allocMillis = allocatable.get(nodeName);
            Integer usedMillis = used.get(nodeName);
            if (usedMillis == null) {
                // no pod is using this node
                usedMillis = 0;
            }
            result.put(nodeName, allocMillis - usedMillis);
        }
        return result;
    }

    Map<String, Integer> getAllocatableCpuMillisByNode() throws UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, IOException {
        Map<String, Integer> result = new HashMap<>();

        List<Node> nodes = cloud.connect().nodes().withLabel(COMPUTE_LABEL_NAME, COMPUTE_LABEL_VALUE).list().getItems();

        for (Node node : nodes) {
            if (isNodeReady(node)) {
                Quantity cpuAlloc = node.getStatus().getAllocatable().get("cpu");
                LOGGER.log(Level.FINE, "node {0} has cpu allocatable {1}", new Object[] {node.getMetadata().getName(), cpuAlloc});
                result.put(node.getMetadata().getName(), calcQuantityMillis(cpuAlloc.getAmount()));
            }
        }

        return result;
    }

    Map<String, Integer> getUsedCpuMillisByNode() throws UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, IOException {
        Map<String, Integer> result = new HashMap<>();

        List<Pod> activeAgentPods = KubernetesCloud.filterActiveAgentPods(cloud.connect().pods().inAnyNamespace().withLabel("jenkins", "slave").list());
        LOGGER.log(Level.FINE, "number of active agent pods: {0}", activeAgentPods.size());

        for (Pod agentPod : activeAgentPods) {
            int podUsedCpuMillis = 0;

            List<Container> containers = agentPod.getSpec().getContainers();
            for (Container container : containers) {
                Map<String, Quantity> requests = container.getResources().getRequests();
                if (requests != null) {
                    Quantity cpuQuantity = requests.get("cpu");
                    if (cpuQuantity != null) {
                        String cpuRequest = cpuQuantity.getAmount();
                        podUsedCpuMillis += calcQuantityMillis(cpuRequest);
                    }
                }
                LOGGER.log(Level.FINE, "container {0} in pod {1} has requests {2} ", new Object[] {container.getName(), agentPod.getMetadata().getName(), requests});
            }
            addCpuMillis(result, agentPod.getSpec().getNodeName(), podUsedCpuMillis);
        }

        return result;
    }

    private void addCpuMillis(Map<String, Integer> cpuMillis, String nodeName, int podUsedCpuMillis) {
        Integer used = cpuMillis.get(nodeName);
        if (used != null) {
            used += podUsedCpuMillis;
        } else {
            used = podUsedCpuMillis;
        }
        cpuMillis.put(nodeName, used);
    }

    private int calcQuantityMillis(String quantity) {
        int quantityMillis = 0;

        try {
            if (quantity.length() == 0) {
                return 0;
            } else if (quantity.endsWith("m")) {
                quantityMillis = Integer.parseInt(quantity.replace("m", ""));
            } else {
                quantityMillis = Integer.parseInt(quantity) * 1000;
            }
        } catch (NumberFormatException e) {
            //ignore, just return 0
        }

        return quantityMillis;
    }
}
