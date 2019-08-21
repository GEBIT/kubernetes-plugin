package org.csanchez.jenkins.plugins.kubernetes;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;

import antlr.ANTLRException;
import hudson.model.Label;
import hudson.model.labels.LabelAtom;
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

    public static ConfigMap createTestConfigMap(boolean locked, int numPendingLaunches) {
        ConfigMap cm = new ConfigMap();
        Map<String, String> data = new HashMap<>();
        data.put(KubernetesCloudLimiter.LOCKED, Boolean.toString(locked));
        data.put(KubernetesCloudLimiter.NUM_PENDING_LAUNCHES, Integer.toString(numPendingLaunches));
        cm.setData(data);

        return cm;
    }

    public static Map<String, Quantity> createCpuQuantity(String cpu) {
        Map<String, Quantity> result = new HashMap<>();

        result.put("cpu", new Quantity(cpu));
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
                    withAllocatable(createCpuQuantity("10")).
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
                    withAllocatable(createCpuQuantity("10")).
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
                    withAllocatable(createCpuQuantity("2")).
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
                    withAllocatable(createCpuQuantity("5")).
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
                    withAllocatable(createCpuQuantity("10")).
                endStatus().
                build();
        result.add(largeReadyNode);

        return result;
    }
    
    public static Pod createTestPod(String name, String nodeName, String cpuReq, String cpuLim, String phase, String jobName) {
        Map<String, Quantity> cpuRequest = createCpuQuantity(cpuReq);
        Map<String, Quantity> cpuLimit = createCpuQuantity(cpuLim);

        Container container = new ContainerBuilder().
                withName(name).
                withNewResources().
                    withRequests(cpuRequest).
                    withLimits(cpuLimit).
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

        // must contribute to used cpu
        result.add(createTestPod("running-slave-1", "small-ready-node", "1", "2", "running", "cloud-project"));
        // must contribute to used cpu
        result.add(createTestPod("running-slave-2", "medium-ready-node", "2", "4", "running", "non-cloud-project"));
        // must contribute to used cpu
        result.add(createTestPod("running-slave-3", "large-ready-node", "1", "2", "running", "cloud-project"));
        // must contribute to used cpu
        result.add(createTestPod("pending-slave-1", "large-ready-node", "3", "6", "pending", "cloud-project"));
        // must NOT contribute to used cpu
        result.add(createTestPod("completed-slave-1", "small-ready-node", "2", "4", "completed", "cloud-project"));

        return result;
    }

    public static PodTemplate createTestPodTemplate(String container1Req, String container2Req) {
        ContainerTemplate milliCpuReqTemplate = new ContainerTemplate("container1", "image1:1");
        milliCpuReqTemplate.setResourceRequestCpu(container1Req);
        ContainerTemplate fullCpuReqTemplate = new ContainerTemplate("container2", "image1:1");
        fullCpuReqTemplate.setResourceRequestCpu(container2Req);

        List<ContainerTemplate> containers = new ArrayList<>();
        containers.add(milliCpuReqTemplate);
        containers.add(fullCpuReqTemplate);

        PodTemplate podTemplate = new PodTemplate();
        podTemplate.setContainers(containers);
        return podTemplate;
    }

    @Test
    public void testGetNumOfPendingLaunchesK8S() throws IOException, UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException {
        ConfigMap cm = createTestConfigMap(false, 3);

        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, IOException, CertificateEncodingException {
                KubernetesClient mockClient =  Mockito.mock(KubernetesClient.class);

                Resource<ConfigMap, DoneableConfigMap> mockResource = Mockito.mock(Resource.class);
                Mockito.when(mockResource.get()).thenReturn(cm);

                MixedOperation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> operation = Mockito.mock(MixedOperation.class);
                Mockito.when(operation.inNamespace(KubernetesCloudLimiter.GLOBAL_NAMESPACE)).thenReturn(operation);
                Mockito.when(operation.withName(KubernetesCloudLimiter.GLOBAL_CONFIG_MAP_NAME)).thenReturn(mockResource);
                
                Mockito.when(mockClient.configMaps()).thenReturn(operation);
                return mockClient;
            }
        };

        KubernetesCloudLimiter limiter = cloud.getLimiter();
        assertEquals("Number of pending launches is wrong", 3, limiter.getNumOfPendingLaunchesK8S());
    }

    @Test
    public void testSetNumOfPendingLaunchesK8S() throws IOException, UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, InterruptedException {
        ConfigMap cm = createTestConfigMap(true, 0);

        DoneableConfigMap dcm = new DoneableConfigMap(cm);

        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, IOException, CertificateEncodingException {
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
        limiter.setNumOfPendingLaunchesK8S(5);
        assertEquals("Number of pending launches is wrong in ConfigMap", "5", dcm.getData().get(KubernetesCloudLimiter.NUM_PENDING_LAUNCHES));
    }

    @Test
    public void testAcquireLockNonForced() throws IOException, UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, InterruptedException {
        ConfigMap cm = createTestConfigMap(false, 0);

        DoneableConfigMap dcm = new DoneableConfigMap(cm);

        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, IOException, CertificateEncodingException {
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
    public void testAcquireLockForced() throws IOException, UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, InterruptedException {
        ConfigMap cm = createTestConfigMap(true, 2);

        DoneableConfigMap dcm = new DoneableConfigMap(cm);

        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, IOException, CertificateEncodingException {
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
    public void testReleaseLock() throws IOException, UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, InterruptedException {
        ConfigMap cm = createTestConfigMap(true, 0);

        DoneableConfigMap dcm = new DoneableConfigMap(cm);

        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, IOException, CertificateEncodingException {
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
    public void testGetAllocatableCpuMillisByNode() throws IOException, UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, InterruptedException {
        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, IOException, CertificateEncodingException {
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
        Map<String, Integer> actual = limiter.getAllocatableCpuMillisByNode();
        Map<String, Integer> expected = new HashMap<>();
        expected.put("small-ready-node", 2000);
        expected.put("medium-ready-node", 5000);
        expected.put("large-ready-node", 10000);

        assertThat("Amount of allocatable cpu is wrong", actual, is(expected));
    }

    @Test
    public void testGetUsedCpuMillisByNode() throws IOException, UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, InterruptedException {
        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, IOException, CertificateEncodingException {
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
        Map<String, Integer> actual = limiter.getUsedCpuMillisByNode();
        Map<String, Integer> expected = new HashMap<>();
        expected.put("small-ready-node", 1000);
        expected.put("medium-ready-node", 2000);
        expected.put("large-ready-node", 4000);

        assertThat("Amount of used cpu is wrong", actual, is(expected));
    }

    @Test
    public void testGetNumSchedulablePods() throws IOException, UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, InterruptedException, ANTLRException {
        Label smallRequest = new LabelAtom("small-request");
        Label largeRequest = new LabelAtom("large-request");

        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, IOException, CertificateEncodingException {
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

              return mockClient;

            }

            @Override
            public PodTemplate getTemplate(Label label) {
                if (label.toString().equals("small-request")) {
                    return createTestPodTemplate("1000m", "1");
                } else {
                    return createTestPodTemplate("2000m", "3");
                }
            }

        };

        KubernetesCloudLimiter limiter = cloud.getLimiter();

        int actual = limiter.getNumSchedulablePods(largeRequest);
        // the large request template has a total cpu request of 5000m. the large-ready-node has 6000m available, so 1 pod can be scheduled
        int expected = 1;
        assertEquals("Amount of schedulable pods is wrong", expected, actual);

        actual = limiter.getNumSchedulablePods(smallRequest);
        // the small request template has a total cpu request of 2000m. the medium-ready-node has 3000m available and the  
        // large-ready-node has 6000m available, so 4 pods can be scheduled
        expected = 4;
        assertEquals("Amount of schedulable pods is wrong", expected, actual);
    }

    @Test
    public void testGetPodTemplateCpuRequestMillis() {

        KubernetesCloud cloud = new KubernetesCloud("name") {

            @Override
            public PodTemplate getTemplate(Label label) {
                return createTestPodTemplate("4000m", "1");
            }
        };
        
        KubernetesCloudLimiter limiter = cloud.getLimiter();
        assertEquals("Amount of requested cpu is wrong", 5000, limiter.getPodTemplateCpuRequestMillis(new LabelAtom("cloud")));
    }
}

