package org.csanchez.jenkins.plugins.kubernetes;

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.csanchez.jenkins.plugins.kubernetes.pod.retention.PodRetention;
import org.csanchez.jenkins.plugins.kubernetes.volumes.EmptyDirVolume;
import org.csanchez.jenkins.plugins.kubernetes.volumes.PodVolume;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.jvnet.hudson.test.JenkinsRule;
import org.jvnet.hudson.test.LoggerRule;
import org.mockito.Mockito;

import hudson.model.Label;
import hudson.slaves.NodeProvisioner;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.DoneableNode;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import jenkins.model.JenkinsLocationConfiguration;

public class KubernetesCloudTest {

    @Rule
    public JenkinsRule j = new JenkinsRule();

    @Rule
    public LoggerRule logs = new LoggerRule().record(Logger.getLogger(KubernetesCloud.class.getPackage().getName()),
            Level.ALL);

    @After
    public void tearDown() {
        System.getProperties().remove("KUBERNETES_JENKINS_URL");
    }

    @Test
    public void testInheritance() {

        ContainerTemplate jnlp = new ContainerTemplate("jnlp", "jnlp:1");
        ContainerTemplate maven = new ContainerTemplate("maven", "maven:1");
        maven.setTtyEnabled(true);
        maven.setCommand("cat");

        PodVolume podVolume = new EmptyDirVolume("/some/path", true);
        PodTemplate parent = new PodTemplate();
        parent.setName("parent");
        parent.setLabel("parent");
        parent.setContainers(Arrays.asList(jnlp));
        parent.setVolumes(Arrays.asList(podVolume));

        ContainerTemplate maven2 = new ContainerTemplate("maven", "maven:2");
        PodTemplate withNewMavenVersion = new PodTemplate();
        withNewMavenVersion.setContainers(Arrays.asList(maven2));

        PodTemplate result = PodTemplateUtils.combine(parent, withNewMavenVersion);
    }

    @Test(expected = IllegalStateException.class)
    public void getJenkinsUrlOrDie_NoJenkinsUrl() {
        JenkinsLocationConfiguration.get().setUrl(null);
        KubernetesCloud cloud = new KubernetesCloud("name");
        String url = cloud.getJenkinsUrlOrDie();
        fail("Should have thrown IllegalStateException at this point but got " + url + " instead.");
    }

    @Test
    public void getJenkinsUrlOrDie_UrlInCloud() {
        System.setProperty("KUBERNETES_JENKINS_URL", "http://mylocationinsysprop");
        KubernetesCloud cloud = new KubernetesCloud("name");
        cloud.setJenkinsUrl("http://mylocation");
        assertEquals("http://mylocation/", cloud.getJenkinsUrlOrDie());
    }

    @Test
    public void getJenkinsUrlOrDie_UrlInSysprop() {
        System.setProperty("KUBERNETES_JENKINS_URL", "http://mylocation");
        KubernetesCloud cloud = new KubernetesCloud("name");
        assertEquals("http://mylocation/", cloud.getJenkinsUrlOrDie());
    }

    @Test
    public void getJenkinsUrlOrDie_UrlInLocation() {
        JenkinsLocationConfiguration.get().setUrl("http://mylocation");
        KubernetesCloud cloud = new KubernetesCloud("name");
        assertEquals("http://mylocation/", cloud.getJenkinsUrlOrDie());
    }

    @Test
    public void testKubernetesCloudDefaults() {
        KubernetesCloud cloud = new KubernetesCloud("name");
        assertEquals(PodRetention.getKubernetesCloudDefault(), cloud.getPodRetention());
    }

    @Test
    public void testLimitedProvision() {
        ConfigMap cm = KubernetesCloudLimiterTest.createTestConfigMap(true, 0);
        DoneableConfigMap dcm = new DoneableConfigMap(cm);

        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, IOException, CertificateEncodingException {
                KubernetesClient mockClient =  Mockito.mock(KubernetesClient.class);

                // mock the global config map
                Resource<ConfigMap, DoneableConfigMap> cmMockResource = Mockito.mock(Resource.class);
                Mockito.when(cmMockResource.get()).thenReturn(cm);
                Mockito.when(cmMockResource.edit()).thenReturn(dcm);

                MixedOperation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> cmOperation = Mockito.mock(MixedOperation.class);
                Mockito.when(cmOperation.inNamespace(KubernetesCloudLimiter.GLOBAL_NAMESPACE)).thenReturn(cmOperation);
                Mockito.when(cmOperation.withName(KubernetesCloudLimiter.GLOBAL_CONFIG_MAP_NAME)).thenReturn(cmMockResource);

                Mockito.when(mockClient.configMaps()).thenReturn(cmOperation);

                // mock the list of available nodes
                NonNamespaceOperation<Node, NodeList, DoneableNode, Resource<Node, DoneableNode>> nodeOperation = Mockito.mock(MixedOperation.class);
                Mockito.when(nodeOperation.withLabel(Mockito.anyString(), Mockito.anyString())).thenReturn(nodeOperation);

                NodeList nodeList = Mockito.mock(NodeList.class);
                Mockito.when(nodeList.getItems()).thenReturn(KubernetesCloudLimiterTest.createTestNodeList());
                Mockito.when(nodeOperation.list()).thenReturn(nodeList);
                Mockito.when(mockClient.nodes()).thenReturn(nodeOperation);

                // mock the list of running pods
                MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> operation = Mockito.mock(MixedOperation.class);

                Mockito.when(operation.inAnyNamespace()).thenReturn(operation);
                Mockito.when(operation.withLabel("jenkins", "slave")).thenReturn(operation);
                PodList podList = Mockito.mock(PodList.class);
                Mockito.when(podList.getItems()).thenReturn(KubernetesCloudLimiterTest.createPodList());
                Mockito.when(operation.list()).thenReturn(podList);
                Mockito.when(mockClient.pods()).thenReturn(operation);

                return mockClient;
            }

            // mock the template for the label
            @Override
            public List<PodTemplate> getTemplatesFor(Label label) {
                List<PodTemplate> result = new ArrayList<>();
                result.add(KubernetesCloudLimiterTest.createTestPodTemplate("1000m", "4"));
                return result;
            }

            // mock the template for the label
            @Override
            public PodTemplate getTemplate(Label label) {
                return KubernetesCloudLimiterTest.createTestPodTemplate("1000m", "4");
            }
        };

        // the cloud has allocatable cpu of 12000m, used cpu of 6000m and is getting a request for 6000m cpu,
        // so exactly one new agent should be planned/pending after the provision call
        Collection<NodeProvisioner.PlannedNode> plannedNodes = cloud.provision(Label.get("test"), 10);
        assertEquals("The number of planned nodes is wrong", 1, plannedNodes.size());
        assertEquals("The number of pending launches is wrong", "1", dcm.getData().get(KubernetesCloudLimiter.NUM_PENDING_LAUNCHES));
        assertEquals("The global config map should not be locked", "false", dcm.getData().get(KubernetesCloudLimiter.LOCKED));

        // now try to provision another agent
        // because there is no more capacity, it should be denied (pending launches count)
        cm.getData().put(KubernetesCloudLimiter.NUM_PENDING_LAUNCHES, "1");
        plannedNodes = cloud.provision(Label.get("test"), 10);
        assertEquals("The number of planned nodes is wrong", 0, plannedNodes.size());
        assertEquals("The number of pending launches is wrong", "1", dcm.getData().get(KubernetesCloudLimiter.NUM_PENDING_LAUNCHES));
        assertEquals("The global config map should not be locked", "false", dcm.getData().get(KubernetesCloudLimiter.LOCKED));
    }
}
