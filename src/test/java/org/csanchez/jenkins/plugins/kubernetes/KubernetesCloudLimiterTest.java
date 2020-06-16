package org.csanchez.jenkins.plugins.kubernetes;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jenkinsci.plugins.kubernetes.auth.KubernetesAuthException;
import org.junit.Test;
import org.mockito.Mockito;

import antlr.ANTLRException;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.DoneableNode;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeBuilder;
import io.fabric8.kubernetes.api.model.NodeCondition;
import io.fabric8.kubernetes.api.model.NodeList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;

public class KubernetesCloudLimiterTest {

    public static ConfigMap createTestConfigMap(boolean locked, int pendingCpuMillis, int pendingMemMi) {
        ConfigMap cm = new ConfigMap();
        Map<String, String> data = new HashMap<>();
        data.put(KubernetesCloudLimiter.LOCKED, Boolean.toString(locked));
        data.put(KubernetesCloudLimiter.PENDING_CPU_MILLIS, Integer.toString(pendingCpuMillis));
        data.put(KubernetesCloudLimiter.PENDING_MEM_MI, Integer.toString(pendingMemMi));
        cm.setData(data);

        return cm;
    }

    public static Map<String, Quantity> createCpuAndMemQuantity(String cpu, String mem) {
        Map<String, Quantity> result = new HashMap<>();

        result.put("cpu", new Quantity(cpu));
        result.put("memory", new Quantity(mem));
        return result;
    }

    public static List<Node> createTestNodeList() {
        List<Node> result = new ArrayList<>();

        // will not be used in calculations for available resources
        Node unschedulableNode = new NodeBuilder().
                withNewMetadata().
                    withName("unschedulable-node").
                endMetadata().
                withNewSpec().
                    withNewUnschedulable(true).
                endSpec().
                withNewStatus().
                    withAllocatable(createCpuAndMemQuantity("10", "128Gi")).
                endStatus().
                build();
        result.add(unschedulableNode);

        // will not be used in calculations for available resources
        Node nonReadyNode = new NodeBuilder().
                withNewMetadata().
                    withName("non-ready-node").
                endMetadata().
                withNewSpec().
                    withNewUnschedulable(false).
                endSpec().
                withNewStatus().
                    withConditions(new NodeCondition("00:00:00", "00:00:00", "message", "reason", "false", "Ready")).
                    withAllocatable(createCpuAndMemQuantity("10", "64Gi")).
                endStatus().
                build();
        result.add(nonReadyNode);

        // small node with 2 cpus only
        Node smallReadyNode = new NodeBuilder().
                withNewMetadata().
                    withName("small-ready-node").
                endMetadata().
                withNewSpec().
                    withNewUnschedulable(false).
                endSpec().
                withNewStatus().
                    withConditions(new NodeCondition("00:00:00", "00:00:00", "message", "reason", "true", "Ready")).
                    withAllocatable(createCpuAndMemQuantity("2", "64Gi")).
                endStatus().
                build();
        result.add(smallReadyNode);

        // medium node with 5 cpus
        Node mediumReadyNode = new NodeBuilder().
                withNewMetadata().
                    withName("medium-ready-node").
                endMetadata().
                withNewSpec().
                    withNewUnschedulable(false).
                endSpec().
                withNewStatus().
                    withConditions(new NodeCondition("00:00:00", "00:00:00", "message", "reason", "true", "Ready")).
                    withAllocatable(createCpuAndMemQuantity("5", "128Gi")).
                endStatus().
                build();
        result.add(mediumReadyNode);

        // large node with 10 cpus
        Node largeReadyNode = new NodeBuilder().
                withNewMetadata().
                    withName("large-ready-node").
                endMetadata().
                withNewSpec().
                    withNewUnschedulable(false).
                endSpec().
                withNewStatus().
                    withConditions(new NodeCondition("00:00:00", "00:00:00", "message", "reason", "true", "Ready")).
                    withAllocatable(createCpuAndMemQuantity("10", "256Gi")).
                endStatus().
                build();
        result.add(largeReadyNode);

        return result;
    }
    
    public static Pod createTestPod(String name, String nodeName, String cpuReq, String cpuLim, String memReq, String memLim, String phase, String jobName) {
        Map<String, Quantity> request = createCpuAndMemQuantity(cpuReq, memReq);
        Map<String, Quantity> limit = createCpuAndMemQuantity(cpuLim, memLim);

        Container container = new ContainerBuilder().
                withName(name).
                withNewResources().
                    withRequests(request).
                    withLimits(limit).
                endResources().
                build();

        Map<String, String> labels = new HashMap<>();
        labels.put("jenkins", "slave");
        labels.put(KubernetesLauncher.JOB_NAME_LABEL, jobName);

        Pod pod = new PodBuilder().
                withNewMetadata().
                    withName(name).
                    withLabels(labels).
                endMetadata().
                withNewSpec().
                    withContainers(container).
                    withNodeName(nodeName).
                endSpec().
                withNewStatus().
                    withPhase(phase).
                endStatus().
                build();
        
        return pod;
    }

    public static List<Pod> createPodList() {
        List<Pod> result = new ArrayList<>();

        // must contribute to used resources
        result.add(createTestPod("running-slave-1", "small-ready-node", "1", "2", "128Mi", "256Mi", "running", "cloud-project"));
        // must contribute to used resources
        result.add(createTestPod("running-slave-2", "medium-ready-node", "2", "4", "1Gi", "2Gi",  "running", "non-cloud-project"));
        // must contribute to used resources
        result.add(createTestPod("running-slave-3", "large-ready-node", "1", "2", "512M", "1024M", "running", "cloud-project"));
        // must contribute to used resources
        result.add(createTestPod("pending-slave-1", "large-ready-node", "3", "6",  "2G", "4G", "pending", "cloud-project"));
        // must NOT contribute to used resources
        result.add(createTestPod("completed-slave-1", "small-ready-node", "2", "4",  "64Mi", "128Mi", "completed", "cloud-project"));

        return result;
    }

    public static PodTemplate createTestPodTemplate(String container1CpuReq, String container2CpuReq, String container1MemReq, String container2MemReq) {
        ContainerTemplate milliCpuReqTemplate = new ContainerTemplate("jnlp", "image1:1");
        milliCpuReqTemplate.setResourceRequestCpu(container1CpuReq);
        milliCpuReqTemplate.setResourceRequestMemory(container1MemReq);
        ContainerTemplate fullCpuReqTemplate = new ContainerTemplate("container2", "image1:1");
        fullCpuReqTemplate.setResourceRequestCpu(container2CpuReq);
        fullCpuReqTemplate.setResourceRequestMemory(container2MemReq);

        List<ContainerTemplate> containers = new ArrayList<>();
        containers.add(milliCpuReqTemplate);
        containers.add(fullCpuReqTemplate);

        PodTemplate podTemplate = new PodTemplate();
        podTemplate.setContainers(containers);
        return podTemplate;
    }

    @Test
    public void testIncPending() throws KubernetesAuthException, IOException {
        ConfigMap cm = createTestConfigMap(false, 3000, 64);
        DoneableConfigMap dcm = new DoneableConfigMap(cm);
        PodTemplate pt = createTestPodTemplate("1", "4.0", "8Gi", "4G");

        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws IOException {
                KubernetesClient mockClient =  Mockito.mock(KubernetesClient.class);

                Resource<ConfigMap, DoneableConfigMap> mockResource = Mockito.mock(Resource.class);
                Mockito.when(mockResource.get()).thenReturn(cm);
                Mockito.when(mockResource.edit()).thenReturn(dcm);

                MixedOperation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> operation = Mockito.mock(MixedOperation.class);
                Mockito.when(operation.inNamespace(KubernetesCloudLimiter.GLOBAL_NAMESPACE)).thenReturn(operation);
                Mockito.when(operation.withName(KubernetesCloudLimiter.GLOBAL_CONFIG_MAP_NAME)).thenReturn(mockResource);
                
                Mockito.when(mockClient.configMaps()).thenReturn(operation);
                return mockClient;
            }
        };

        cloud.getLimiter().incPending(pt);
        // must be 3000m from config map + 1000m from first container and + 4000m from second container = 8000m
        assertEquals("Number of pending cpu millis is wrong", 8000, Integer.parseInt(dcm.getData().get(KubernetesCloudLimiter.PENDING_CPU_MILLIS)));
        // must be 64Mi from config map + 8Gi (= 8192Mi) from first container and + 4G (= 3814Mi) from second container = 12070Mi
        assertEquals("Number of pending mem Mi is wrong", 12070, Integer.parseInt(dcm.getData().get(KubernetesCloudLimiter.PENDING_MEM_MI)));
    }

    @Test
    public void testDecPending() throws KubernetesAuthException, IOException {
        ConfigMap cm = createTestConfigMap(false, 10000, 128000);
        DoneableConfigMap dcm = new DoneableConfigMap(cm);
        PodTemplate pt = createTestPodTemplate(".9", "4000m", "2000Mi", "4000M");

        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws IOException {
                KubernetesClient mockClient =  Mockito.mock(KubernetesClient.class);

                Resource<ConfigMap, DoneableConfigMap> mockResource = Mockito.mock(Resource.class);
                Mockito.when(mockResource.get()).thenReturn(cm);
                Mockito.when(mockResource.edit()).thenReturn(dcm);

                MixedOperation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> operation = Mockito.mock(MixedOperation.class);
                Mockito.when(operation.inNamespace(KubernetesCloudLimiter.GLOBAL_NAMESPACE)).thenReturn(operation);
                Mockito.when(operation.withName(KubernetesCloudLimiter.GLOBAL_CONFIG_MAP_NAME)).thenReturn(mockResource);
                
                Mockito.when(mockClient.configMaps()).thenReturn(operation);
                return mockClient;
            }
        };

        cloud.getLimiter().decPending(pt);
        // must be 10000m from config map - 900m from first container and - 4000m from second container = 5100m
        assertEquals("Number of pending cpu millis is wrong", 5100, Integer.parseInt(dcm.getData().get(KubernetesCloudLimiter.PENDING_CPU_MILLIS)));
        // must be 128000Mi from config map - 2000Mi from first container and - 4000M (= 3814Mi) from second container = 122186Mi 
        assertEquals("Number of pending mem Mi is wrong", 122186, Integer.parseInt(dcm.getData().get(KubernetesCloudLimiter.PENDING_MEM_MI)));
    }

    @Test
    public void testAcquireLockNonForced() throws KubernetesAuthException, IOException, InterruptedException {
        ConfigMap cm = createTestConfigMap(false, 0, 64);

        DoneableConfigMap dcm = new DoneableConfigMap(cm);

        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws IOException {
                KubernetesClient mockClient =  Mockito.mock(KubernetesClient.class);

                Resource<ConfigMap, DoneableConfigMap> mockResource = Mockito.mock(Resource.class);
                Mockito.when(mockResource.get()).thenReturn(cm);
                Mockito.when(mockResource.edit()).thenReturn(dcm);

                MixedOperation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> operation = Mockito.mock(MixedOperation.class);
                Mockito.when(operation.inNamespace(KubernetesCloudLimiter.GLOBAL_NAMESPACE)).thenReturn(operation);
                Mockito.when(operation.withName(KubernetesCloudLimiter.GLOBAL_CONFIG_MAP_NAME)).thenReturn(mockResource);

                Mockito.when(mockClient.configMaps()).thenReturn(operation);
                return mockClient;
            }
        };

        KubernetesCloudLimiter limiter = cloud.getLimiter();
        boolean result = limiter.acquireLock();
        assertEquals("Non-forced lock has not been acquired", true, result);
        assertEquals("Non-forced lock acquirement has not been written to ConfigMap", "true", dcm.getData().get(KubernetesCloudLimiter.LOCKED));
    }

    @Test
    public void testAcquireLockForced() throws KubernetesAuthException, IOException, InterruptedException {
        ConfigMap cm = createTestConfigMap(true, 2, 64);

        DoneableConfigMap dcm = new DoneableConfigMap(cm);

        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws IOException {
                KubernetesClient mockClient =  Mockito.mock(KubernetesClient.class);

                Resource<ConfigMap, DoneableConfigMap> mockResource = Mockito.mock(Resource.class);
                Mockito.when(mockResource.get()).thenReturn(cm);
                Mockito.when(mockResource.edit()).thenReturn(dcm);

                MixedOperation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> operation = Mockito.mock(MixedOperation.class);
                Mockito.when(operation.inNamespace(KubernetesCloudLimiter.GLOBAL_NAMESPACE)).thenReturn(operation);
                Mockito.when(operation.withName(KubernetesCloudLimiter.GLOBAL_CONFIG_MAP_NAME)).thenReturn(mockResource);

                Mockito.when(mockClient.configMaps()).thenReturn(operation);
                return mockClient;
            }
        };

        KubernetesCloudLimiter limiter = cloud.getLimiter();
        boolean result = limiter.acquireLock();
        assertEquals("Forced lock has not been acquired", true, result);
        assertEquals("Force lock acquirement has not been written to ConfigMap", "true", dcm.getData().get(KubernetesCloudLimiter.LOCKED));
    }

    @Test
    public void testReleaseLock() throws KubernetesAuthException, IOException {
        ConfigMap cm = createTestConfigMap(true, 0, 64);

        DoneableConfigMap dcm = new DoneableConfigMap(cm);

        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws IOException {
                KubernetesClient mockClient =  Mockito.mock(KubernetesClient.class);

                Resource<ConfigMap, DoneableConfigMap> mockResource = Mockito.mock(Resource.class);
                Mockito.when(mockResource.get()).thenReturn(cm);
                Mockito.when(mockResource.edit()).thenReturn(dcm);

                MixedOperation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> operation = Mockito.mock(MixedOperation.class);
                Mockito.when(operation.inNamespace(KubernetesCloudLimiter.GLOBAL_NAMESPACE)).thenReturn(operation);
                Mockito.when(operation.withName(KubernetesCloudLimiter.GLOBAL_CONFIG_MAP_NAME)).thenReturn(mockResource);

                Mockito.when(mockClient.configMaps()).thenReturn(operation);
                return mockClient;
            }
        };

        KubernetesCloudLimiter limiter = cloud.getLimiter();
        limiter.releaseLock();
        assertEquals("Lock release has not been written to ConfigMap", "false", dcm.getData().get(KubernetesCloudLimiter.LOCKED));
    }

    @Test
    public void testGetAllocatableCpuMillis() throws KubernetesAuthException, IOException  {
        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws IOException {
                KubernetesClient mockClient =  Mockito.mock(KubernetesClient.class);

                NonNamespaceOperation<Node, NodeList, DoneableNode, Resource<Node, DoneableNode>> operation = Mockito.mock(MixedOperation.class);
                Mockito.when(operation.withLabel(Mockito.anyString(), Mockito.anyString())).thenReturn(operation);

                NodeList nodeList = Mockito.mock(NodeList.class);
                Mockito.when(nodeList.getItems()).thenReturn(createTestNodeList());
                Mockito.when(operation.list()).thenReturn(nodeList);
                Mockito.when(mockClient.nodes()).thenReturn(operation);
                return mockClient;
            }
        };

        KubernetesCloudLimiter limiter = cloud.getLimiter();
        int actual = limiter.getAllocatableCpuMillis();
        // sum of all allocatable cpu over all nodes
        int expected = 17000;

        assertEquals("Amount of allocatable cpu is wrong", expected, actual);
    }

    @Test
    public void testGetAllocatableMemMi() throws KubernetesAuthException, IOException {
        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws IOException {
                KubernetesClient mockClient =  Mockito.mock(KubernetesClient.class);

                NonNamespaceOperation<Node, NodeList, DoneableNode, Resource<Node, DoneableNode>> operation = Mockito.mock(MixedOperation.class);
                Mockito.when(operation.withLabel(Mockito.anyString(), Mockito.anyString())).thenReturn(operation);

                NodeList nodeList = Mockito.mock(NodeList.class);
                Mockito.when(nodeList.getItems()).thenReturn(createTestNodeList());
                Mockito.when(operation.list()).thenReturn(nodeList);
                Mockito.when(mockClient.nodes()).thenReturn(operation);
                return mockClient;
            }
        };

        KubernetesCloudLimiter limiter = cloud.getLimiter();
        int actual = limiter.getAllocatableMemMi();
        // sum of all allocatable mem over all nodes
        int expected = 458752;

        assertEquals("Amount of allocatable mem is wrong", expected, actual);
    }

    @Test
    public void testGetUsedCpuMillis() throws KubernetesAuthException, IOException {
        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws IOException {
              KubernetesClient mockClient =  Mockito.mock(KubernetesClient.class);

              MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> operation = Mockito.mock(MixedOperation.class);

              Mockito.when(operation.inAnyNamespace()).thenReturn(operation);
              Mockito.when(operation.withLabel("jenkins", "slave")).thenReturn(operation);
              PodList podList = Mockito.mock(PodList.class);
              Mockito.when(podList.getItems()).thenReturn(createPodList());
              Mockito.when(operation.list()).thenReturn(podList);
              Mockito.when(mockClient.pods()).thenReturn(operation);
              return mockClient;

            }
        };

        KubernetesCloudLimiter limiter = cloud.getLimiter();
        int actual = limiter.getUsedCpuMillis();
        // sum of all used cpu over all nodes
        int expected = 7000;

        assertEquals("Amount of used cpu is wrong", expected, actual);
    }

    @Test
    public void testGetUsedMemMi() throws KubernetesAuthException, IOException {
        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws IOException {
              KubernetesClient mockClient =  Mockito.mock(KubernetesClient.class);

              MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> operation = Mockito.mock(MixedOperation.class);

              Mockito.when(operation.inAnyNamespace()).thenReturn(operation);
              Mockito.when(operation.withLabel("jenkins", "slave")).thenReturn(operation);
              PodList podList = Mockito.mock(PodList.class);
              Mockito.when(podList.getItems()).thenReturn(createPodList());
              Mockito.when(operation.list()).thenReturn(podList);
              Mockito.when(mockClient.pods()).thenReturn(operation);
              return mockClient;

            }
        };

        KubernetesCloudLimiter limiter = cloud.getLimiter();
        int actual = limiter.getUsedMemMi();
        // sum of all used mem over all nodes
        int expected = 3547;

        assertEquals("Amount of used mem is wrong", expected, actual);
    }

    @Test
    public void testEstimateNumSchedulablePods() throws KubernetesAuthException, IOException {
        ConfigMap cm = createTestConfigMap(true, 0, 64);

        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws IOException {
              KubernetesClient mockClient =  Mockito.mock(KubernetesClient.class);

              MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> operation = Mockito.mock(MixedOperation.class);

              Mockito.when(operation.inAnyNamespace()).thenReturn(operation);
              Mockito.when(operation.withLabel("jenkins", "slave")).thenReturn(operation);
              PodList podList = Mockito.mock(PodList.class);
              Mockito.when(podList.getItems()).thenReturn(createPodList());
              Mockito.when(operation.list()).thenReturn(podList);
              Mockito.when(mockClient.pods()).thenReturn(operation);

              NonNamespaceOperation<Node, NodeList, DoneableNode, Resource<Node, DoneableNode>> nodeOperation = Mockito.mock(MixedOperation.class);
              Mockito.when(nodeOperation.withLabel(Mockito.anyString(), Mockito.anyString())).thenReturn(nodeOperation);

              NodeList nodeList = Mockito.mock(NodeList.class);
              Mockito.when(nodeList.getItems()).thenReturn(createTestNodeList());
              Mockito.when(nodeOperation.list()).thenReturn(nodeList);
              Mockito.when(mockClient.nodes()).thenReturn(nodeOperation);

              Resource<ConfigMap, DoneableConfigMap> mockResource = Mockito.mock(Resource.class);
              Mockito.when(mockResource.get()).thenReturn(cm);

              MixedOperation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> cmOperation = Mockito.mock(MixedOperation.class);
              Mockito.when(cmOperation.inNamespace(KubernetesCloudLimiter.GLOBAL_NAMESPACE)).thenReturn(cmOperation);
              Mockito.when(cmOperation.withName(KubernetesCloudLimiter.GLOBAL_CONFIG_MAP_NAME)).thenReturn(mockResource);

              Mockito.when(mockClient.configMaps()).thenReturn(cmOperation);

              return mockClient;

            }

        };

        PodTemplate smallTemplate = createTestPodTemplate("1000m", "1", "1Gi", "1Gi");
        PodTemplate largeTemplate = createTestPodTemplate("1000m", "500m", "32G", "64000000Ki");
        PodTemplate tooLargeTemplate = createTestPodTemplate("10", "10", "1Gi", "1Gi");
        PodTemplate zeroCpuReqTemplate = createTestPodTemplate("0", "0", "1Gi", "1Gi");
        PodTemplate zeroMemReqTemplate = createTestPodTemplate("1", "1", "0", "0");

        KubernetesCloudLimiter limiter = cloud.getLimiter();

        int actual = limiter.estimateNumSchedulablePods(smallTemplate);
        // the small request template has a total cpu request of 2000m and total mem request of 2048Mi.
        // the cluster has 17000m allocatable cpu of which 7000m are used, so 10000m cpu are available.
        // the cluster has 458752Mi allocatable mem of which are 3547Mi used, so 455205Mi mem are available.
        // the limiting factor is the cpu here, expected schedulable pods is 5
        int expected = 10000 / 2000;
        assertEquals("Amount of schedulable pods is wrong", expected, actual);

        actual = limiter.estimateNumSchedulablePods(largeTemplate);
        // the large request template has a total cpu request of 1500m and total mem request of 30517Mi + 62500Mi = 93017Mi.
        // the cluster has 17000m allocatable cpu of which 7000m are used, so 10000m cpu are available.
        // the cluster has 458752Mi allocatable mem of which are 3547Mi used, so 455205Mi mem are available.
        // the limiting factor is the mem here, expected schedulable pods is 4
        expected = 455205 / 93017;
        assertEquals("Amount of schedulable pods is wrong", expected, actual);

        actual = limiter.estimateNumSchedulablePods(tooLargeTemplate);
        // the large request template has a total cpu request of 20000m and total mem request of 2048Mi.
        // the cluster has 17000m allocatable cpu of which 7000m are used, so 10000m cpu are available.
        // the cluster has 458752Mi allocatable mem of which are 3547Mi used, so 455205Mi mem are available.
        // the limiting factor is the cpu here, expected schedulable pods is 0
        expected = 0;
        assertEquals("Amount of schedulable pods is wrong", expected, actual);

        actual = limiter.estimateNumSchedulablePods(zeroCpuReqTemplate);
        // zero requests will not be scheduled
        expected = 0;
        assertEquals("Amount of schedulable pods is wrong", expected, actual);

        actual = limiter.estimateNumSchedulablePods(zeroMemReqTemplate);
        assertEquals("Amount of schedulable pods is wrong", expected, actual);
    }
}

