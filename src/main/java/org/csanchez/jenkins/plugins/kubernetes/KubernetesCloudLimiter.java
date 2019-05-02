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

    private static final String COMPUTE_LABEL_VALUE = "general";
    private static final String COMPUTE_LABEL_NAME = "compute";
    private static final String GLOBAL_CONFIG_MAP_NAME = "default";
    private static final String LOCKED = "locked";
    private static final String GLOBAL_NAMESPACE = "jenkins-masters";
    private static final String NUM_PENDING_LAUNCHES = "numPendingLaunches";
    private static final long MAX_ĹOCK_TIME_MS = 60 * 1000;
    private static final int MAX_TRIES = 5;

    private KubernetesCloud cloud;

    private long lastSuccessfulLock;

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

        if (lastSuccessfulLock == 0) {
            //first try to acquire the lock, init lastSuccessfulLock, so we dont force from the start
            lastSuccessfulLock = System.currentTimeMillis();
        }
        int numTries = 0;
        while (numTries < MAX_TRIES) {
            //get the global config map
            ConfigMap cm = getGlobalConfigMap();
            if (cm == null) {
                LOGGER.log(Level.SEVERE, "global config map does not exist");
                return false;
            }
            boolean locked = Boolean.parseBoolean(cm.getData().get(LOCKED));
            if (locked && !force) {
                //wait a second, then try again
                Thread.sleep(1000);
                if ((System.currentTimeMillis() - lastSuccessfulLock) > MAX_ĹOCK_TIME_MS) {
                    //lock was held for too long, force it open
                    acquireLock(true);
                }
            } else {
                //we lock for us now
                findGlobalConfigMap().edit().addToData(LOCKED, "true").done();
                lastSuccessfulLock = System.currentTimeMillis();
                return true;
            }
            numTries++;
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

    public int getAllocatableCpuMillis() throws UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, IOException {
        List<Node> nodes = cloud.connect().nodes().withLabel(COMPUTE_LABEL_NAME, COMPUTE_LABEL_VALUE).list().getItems();

        int totalAllocatableCpuMillis = 0;

        for (Node node : nodes) {
            if (isNodeReady(node)) {
                Quantity cpuAlloc = node.getStatus().getAllocatable().get("cpu");
                LOGGER.log(Level.FINE, "node {0} has cpu allocatable {1}", new Object[] {node.getMetadata().getName(), cpuAlloc});

                totalAllocatableCpuMillis += Integer.parseInt(cpuAlloc.getAmount()) * 1000;
            }
        }

        return totalAllocatableCpuMillis;
    }

    public int getUsedCpuMillis() throws UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, IOException {
        List<Pod> activeAgentPods = KubernetesCloud.filterActiveAgentPods(cloud.connect().pods().inAnyNamespace().withLabel("jenkins", "slave").list());
        LOGGER.log(Level.FINE, "number of active agent pods: {0}", activeAgentPods.size());

        int totalUsedCpuMillis = 0;

        for (Pod agentPod : activeAgentPods) {
            List<Container> containers = agentPod.getSpec().getContainers();
            for (Container container : containers) {
                Map<String, Quantity> requests = container.getResources().getRequests();
                if (requests != null) {
                    Quantity cpuQuantity = requests.get("cpu");
                    if (cpuQuantity != null) {
                        String cpuRequest = cpuQuantity.getAmount();
                        totalUsedCpuMillis += calcQuantityMillis(cpuRequest);
                    }
                }
                LOGGER.log(Level.FINE, "container {0} in pod {1} has requests {2} ", new Object[] {container.getName(), agentPod.getMetadata().getName(), requests});
            }
        }

        return totalUsedCpuMillis;
    }

    public int getPodTemplateCpuRequestMillis(Label label) {
        int podTemplateCpuRequestMillis = 0;

        PodTemplate podTemplate = cloud.getTemplate(label);

        for (ContainerTemplate containerTemplate : podTemplate.getContainers()) {
            podTemplateCpuRequestMillis += calcQuantityMillis(containerTemplate.getResourceRequestCpu());
        }

        return podTemplateCpuRequestMillis;
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
