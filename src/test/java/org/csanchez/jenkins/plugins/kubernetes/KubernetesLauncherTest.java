package org.csanchez.jenkins.plugins.kubernetes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateEncodingException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.jvnet.hudson.test.JenkinsRule;
import org.mockito.Mockito;

import hudson.model.FreeStyleProject;
import hudson.model.Label;
import hudson.model.Queue;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HostPathVolumeSource;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSource;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;

public class KubernetesLauncherTest {

    @Rule
    public JenkinsRule r = new JenkinsRule();

    private FreeStyleProject cloudProject;
    private FreeStyleProject nonCloudProject;

    private KubernetesLauncher launcher = new KubernetesLauncher();

    @Before
    public void setup() throws IOException {
        if (cloudProject == null) {
            cloudProject = r.createFreeStyleProject("cloud-project");
            cloudProject.setAssignedLabel(Label.get("cloud-label"));
        }

        if (nonCloudProject == null) {
            nonCloudProject = r.createFreeStyleProject("non-cloud-project");
            nonCloudProject.setAssignedLabel(Label.get("non-cloud-label"));
        }
    }

    private List<Queue.BuildableItem> createBuildableItemList() throws IOException {
        List<Queue.BuildableItem> result = new ArrayList<>();

        Queue.WaitingItem waitingItem = new Queue.WaitingItem(Calendar.getInstance(), cloudProject, new ArrayList<>());
        result.add(new Queue.BuildableItem(waitingItem));

        waitingItem = new Queue.WaitingItem(Calendar.getInstance(), nonCloudProject, new ArrayList<>());
        result.add(new Queue.BuildableItem(waitingItem));

        return result;
    }

    @Test
    public void testFindFirstBuildableToProvisionForAlreadyBuilding() throws UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, IOException {
        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, IOException, CertificateEncodingException {
                KubernetesClient mockClient =  Mockito.mock(KubernetesClient.class);

                // mock the list of running pods
                MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> operation = Mockito.mock(MixedOperation.class);

                Mockito.when(operation.inNamespace(Mockito.anyString())).thenReturn(operation);
                Mockito.when(operation.withLabel(Mockito.anyString(), Mockito.anyString())).thenReturn(operation);
                PodList podList = Mockito.mock(PodList.class);
                Mockito.when(podList.getItems()).thenReturn(KubernetesCloudLimiterTest.createPodList());
                Mockito.when(operation.list()).thenReturn(podList);
                Mockito.when(mockClient.pods()).thenReturn(operation);

                return mockClient;
            }
        };

        assertNull("There should not be a buildable to provision for, because a build is already running", launcher.findFirstBuildableToProvisionFor(createBuildableItemList(), "pos", "cloud-label", cloud.connect()));
    }

    @Test
    public void testFindFirstBuildableToProvisionForNotAlreadyBuilding() throws UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, IOException {
        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, IOException, CertificateEncodingException {
                KubernetesClient mockClient =  Mockito.mock(KubernetesClient.class);

                // mock the list of running pods
                MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> operation = Mockito.mock(MixedOperation.class);

                Mockito.when(operation.inNamespace(Mockito.anyString())).thenReturn(operation);
                Mockito.when(operation.withLabel(Mockito.anyString(), Mockito.anyString())).thenReturn(operation);
                PodList podList = Mockito.mock(PodList.class);

                // empty list of running pods (for the label)
                Mockito.when(podList.getItems()).thenReturn(new ArrayList<>());
                Mockito.when(operation.list()).thenReturn(podList);
                Mockito.when(mockClient.pods()).thenReturn(operation);

                return mockClient;
            }
        };

        assertNotNull("There should be a buildable to provision for", launcher.findFirstBuildableToProvisionFor(createBuildableItemList(), "pos", "cloud-label", cloud.connect()));
    }

    @Test
    public void testFindFirstBuildableToProvisionForNonMatchingLabel() throws UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, IOException {
        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException, IOException, CertificateEncodingException {
                KubernetesClient mockClient =  Mockito.mock(KubernetesClient.class);

                // mock the list of running pods
                MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> operation = Mockito.mock(MixedOperation.class);

                Mockito.when(operation.inNamespace(Mockito.anyString())).thenReturn(operation);
                Mockito.when(operation.withLabel(Mockito.anyString(), Mockito.anyString())).thenReturn(operation);
                PodList podList = Mockito.mock(PodList.class);

                // empty list of running pods (for the label)
                Mockito.when(podList.getItems()).thenReturn(new ArrayList<>());
                Mockito.when(operation.list()).thenReturn(podList);
                Mockito.when(mockClient.pods()).thenReturn(operation);

                return mockClient;
            }
        };

        assertNull("There should be no buildable to provision for, because the labels don't match", launcher.findFirstBuildableToProvisionFor(createBuildableItemList(), "pos", "non-matching-label", cloud.connect()));
    }

    @Test
    public void testAdjustPodToBuildable() throws UnrecoverableKeyException, CertificateEncodingException, NoSuchAlgorithmException, KeyStoreException, IOException {
        Pod pod = KubernetesCloudLimiterTest.createTestPod("jnlp", "node1", "1", "2", "running", "cloud-project");
        // add a host path volume for adjusting (the workspace volume) 
        pod.getSpec().getVolumes().add(new VolumeBuilder().withName("hostpath-volume").withHostPath(new HostPathVolumeSource("/var/test/path", "type")).build());
        // add a pvc for adjusting (the home volume)
        pod.getSpec().getVolumes().add(new VolumeBuilder().withName(PodTemplateBuilder.HOME_VOLUME_NAME).withPersistentVolumeClaim(new PersistentVolumeClaimVolumeSource("job-name", false)).build());
        
        Queue.BuildableItem item = new Queue.BuildableItem(new Queue.WaitingItem(Calendar.getInstance(), cloudProject, new ArrayList<>()));

        String namespace = "pos";
        launcher.adjustPodToBuildable(pod, item, namespace);

        assertEquals("Pod has wrong JOB_NAME_LABEL", pod.getMetadata().getLabels().get(KubernetesLauncher.JOB_NAME_LABEL), cloudProject.getName());

        EnvVar expectedFullJobName = new EnvVarBuilder().withName("FULL_JOB_NAME").withValue("cloud-project/").build();
        assertEquals("Container has wrong FULL_JOB_NAME", expectedFullJobName, pod.getSpec().getContainers().get(0).getEnv().get(0));

        // expect the hostpath volume path to be extended by the namespace and the hashed name
        String expectedHostPath = "/var/test/path/" + namespace + "/" + KubernetesLauncher.calcHashedVolumeName("job", cloudProject.getName());
        assertEquals("Pod has wrong hostPath volume path", expectedHostPath, pod.getSpec().getVolumes().get(0).getHostPath().getPath());

        // expect the claimName of the pvc to be changed to the hashed name
        String expectedClaimName = KubernetesLauncher.calcHashedVolumeName("job", cloudProject.getName());
        assertEquals("Pod has wrong hostPath volume path", expectedClaimName, pod.getSpec().getVolumes().get(1).getPersistentVolumeClaim().getClaimName());
    }
}

