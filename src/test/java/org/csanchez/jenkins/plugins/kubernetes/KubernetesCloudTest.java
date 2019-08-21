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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.ImmutableMap;
import org.csanchez.jenkins.plugins.kubernetes.pod.retention.Always;
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
    public void testPodLabels() {
        List<PodLabel> defaultPodLabelsList = PodLabel.fromMap(KubernetesCloud.DEFAULT_POD_LABELS);
        KubernetesCloud cloud = new KubernetesCloud("name");
        assertEquals(KubernetesCloud.DEFAULT_POD_LABELS, cloud.getPodLabelsMap());
        assertEquals(defaultPodLabelsList, cloud.getPodLabels());
        assertEquals(cloud.getPodLabelsMap(), cloud.getLabels());

        List<PodLabel> labels = PodLabel.listOf("foo", "bar", "cat", "dog");
        cloud.setPodLabels(labels);
        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("foo", "bar");
        expected.put("cat", "dog");
        assertEquals(expected, cloud.getPodLabelsMap());
        assertEquals(cloud.getPodLabelsMap(), cloud.getLabels());
        assertEquals(new ArrayList<>(labels), cloud.getPodLabels());

        cloud.setPodLabels(null);
        assertEquals(KubernetesCloud.DEFAULT_POD_LABELS, cloud.getPodLabelsMap());
        assertEquals(defaultPodLabelsList, cloud.getPodLabels());

        cloud.setPodLabels(new ArrayList<>());
        assertEquals(KubernetesCloud.DEFAULT_POD_LABELS, cloud.getPodLabelsMap());
        assertEquals(cloud.getPodLabelsMap(), cloud.getLabels());
        assertEquals(defaultPodLabelsList, cloud.getPodLabels());
    }

    @Test
    public void testLabels() {
        KubernetesCloud cloud = new KubernetesCloud("name");

        List<PodLabel> labels = PodLabel.listOf("foo", "bar", "cat", "dog");
        cloud.setPodLabels(labels);
        Map<String, String> labelsMap = new LinkedHashMap<>();
        for (PodLabel l : labels) {
            labelsMap.put(l.getKey(), l.getValue());
        }
        cloud.setLabels(labelsMap);
        assertEquals(new LinkedHashMap<>(labelsMap), cloud.getPodLabelsMap());
        assertEquals(labels, cloud.getPodLabels());


        cloud.setLabels(null);
        assertEquals(ImmutableMap.of("jenkins", "slave"), cloud.getPodLabelsMap());
        assertEquals(ImmutableMap.of("jenkins", "slave"), cloud.getLabels());

        cloud.setLabels(new LinkedHashMap<>());
        assertEquals(ImmutableMap.of("jenkins", "slave"), cloud.getPodLabelsMap());
        assertEquals(ImmutableMap.of("jenkins", "slave"), cloud.getLabels());
    }

    @Test
    public void copyConstructor() {
        PodTemplate pt = new PodTemplate();
        pt.setName("podTemplate");

        KubernetesCloud cloud = new KubernetesCloud("name");
        cloud.setDefaultsProviderTemplate("default");
        cloud.setTemplates(Collections.singletonList(pt));
        cloud.setServerUrl("serverUrl");
        cloud.setSkipTlsVerify(true);
        cloud.setAddMasterProxyEnvVars(true);
        cloud.setNamespace("namespace");
        cloud.setJenkinsUrl("jenkinsUrl");
        cloud.setJenkinsTunnel("tunnel");
        cloud.setCredentialsId("abcd");
        cloud.setRetentionTimeout(1000);
        cloud.setConnectTimeout(123);
        cloud.setUsageRestricted(true);
        cloud.setMaxRequestsPerHostStr("42");
        cloud.setPodRetention(new Always());
        cloud.setWaitForPodSec(245);
        cloud.setPodLabels(PodLabel.listOf("foo", "bar", "cat", "dog"));

        KubernetesCloud copy = new KubernetesCloud("copy", cloud);

        assertEquals("Expected cloud from copy constructor to be equal to the source except for name", cloud, copy);
    }

}
