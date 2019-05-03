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

import org.junit.Test;
import org.mockito.Mockito;

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
                    withAllocatable(createCpuQuantity("1")).
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
                    withAllocatable(createCpuQuantity("1")).
                endStatus().
                build();
        result.add(nonReadyNode);

        // must contribute to total allocatable
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

        // must contribute to total allocatable
        Node bigReadyNode = new NodeBuilder().
                withNewMetadata().
                    withName("big-ready-node").
                endMetadata().
                withNewSpec().
                    withNewUnschedulable(false).
                endSpec().
                withNewStatus().
                    withConditions(new NodeCondition("00:00:00", "00:00:00", "message", "reason", "true", "Ready")).
                    withAllocatable(createCpuQuantity("10")).
                endStatus().
                build();
        result.add(bigReadyNode);

        return result;
    }
    
    public static Pod createTestPod(String name, String cpuReq, String cpuLim, String phase, String jobName) {
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
        result.add(createTestPod("running-slave-1", "1", "2", "running", "cloud-project"));
        // must contribute to used cpu
        result.add(createTestPod("running-slave-2", "2", "4", "running", "non-cloud-project"));
        // must contribute to used cpu
        result.add(createTestPod("pending-slave-2", "3", "6", "pending", "cloud-project"));
        // must NOT contribute to used cpu
        result.add(createTestPod("completed-slave-1", "2", "4", "completed", "cloud-project"));

        return result;
    }

    public static PodTemplate createTestPodTemplate() {
        ContainerTemplate milliCpuReqTemplate = new ContainerTemplate("container1", "image1:1");
        milliCpuReqTemplate.setResourceRequestCpu("1000m");
        ContainerTemplate fullCpuReqTemplate = new ContainerTemplate("container2", "image1:1");
        fullCpuReqTemplate.setResourceRequestCpu("4");

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
    public void testGetAllocatableCpuMillis() throws IOException, UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, InterruptedException {
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
        assertEquals("Amount of allocatable cpu is wrong", 12000, limiter.getAllocatableCpuMillis());
    }

    @Test
    public void testGetUsedCpuMillis() throws IOException, UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, InterruptedException {
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
        assertEquals("Amount of used cpu is wrong", 6000, limiter.getUsedCpuMillis());
    }

    @Test
    public void testGetPodTemplateCpuRequestMillis() {

        KubernetesCloud cloud = new KubernetesCloud("name") {

            @Override
            public PodTemplate getTemplate(Label label) {
                return createTestPodTemplate();
            }
        };
        
        KubernetesCloudLimiter limiter = cloud.getLimiter();
        assertEquals("Amount of requested cpu is wrong", 5000, limiter.getPodTemplateCpuRequestMillis(new LabelAtom("cloud")));
    }
}

