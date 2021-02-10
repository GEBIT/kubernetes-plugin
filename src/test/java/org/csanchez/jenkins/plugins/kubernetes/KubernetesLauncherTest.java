package org.csanchez.jenkins.plugins.kubernetes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;

import org.jenkinsci.plugins.kubernetes.auth.KubernetesAuthException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.jvnet.hudson.test.JenkinsRule;
import org.mockito.Mockito;

import hudson.matrix.Axis;
import hudson.matrix.AxisList;
import hudson.matrix.MatrixConfiguration;
import hudson.matrix.MatrixProject;
import hudson.maven.MavenModuleSet;
import hudson.model.Label;
import hudson.model.Queue;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;

public class KubernetesLauncherTest {

    @Rule
    public JenkinsRule r = new JenkinsRule();

    private MavenModuleSet cloudProject;
    private MavenModuleSet nonCloudProject;
    
    private MavenModuleSet mavenProject;
    private MatrixProject matrixProject;

    private KubernetesLauncher launcher = new KubernetesLauncher();

    @Before
    public void setup() throws IOException {
        if (mavenProject == null) {
            mavenProject = r.createProject(MavenModuleSet.class, "maven-cloud-project");
            mavenProject.setAssignedLabel(Label.get("maven-cloud-label"));
        }

        if (matrixProject == null) {
            matrixProject = r.createProject(MatrixProject.class, "matrix-cloud-project");
            matrixProject.setAssignedLabel(Label.get("matrix-cloud-label"));
            matrixProject.setAxes(new AxisList(new Axis("JDK", "jdk8", "jdk11")));
        }

        if (cloudProject == null) {
            cloudProject = r.createProject(MavenModuleSet.class, "cloud-project");
            cloudProject.setAssignedLabel(Label.get("cloud-label"));
        }

        if (nonCloudProject == null) {
            nonCloudProject = r.createProject(MavenModuleSet.class,"non-cloud-project");
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
    public void testFindFirstBuildableToProvisionForAlreadyBuilding() throws IOException, KubernetesAuthException {
        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws IOException {
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
    public void testFindFirstBuildableToProvisionForNotAlreadyBuilding() throws IOException, KubernetesAuthException {
        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws IOException {
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
    public void testFindFirstBuildableToProvisionForNonMatchingLabel() throws IOException, KubernetesAuthException {
        KubernetesCloud cloud = new KubernetesCloud("name") {
            @Override
            public KubernetesClient connect() throws IOException {
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
    public void testaddPodLabeAndlJnlpEnvVarsMavenProject() {
        Pod pod = KubernetesCloudLimiterTest.createTestPod("jnlp", "node1", "1", "2", "1Gi", "2Gi", "running", "maven-cloud-project");
        Queue.BuildableItem item = new Queue.BuildableItem(new Queue.WaitingItem(Calendar.getInstance(), mavenProject, new ArrayList<>()));

        String namespace = "pos";
        launcher.addPodLabeAndlJnlpEnvVars(pod, item, namespace);

        assertEquals("Pod has wrong JOB_NAME_LABEL", pod.getMetadata().getLabels().get(KubernetesLauncher.JOB_NAME_LABEL), mavenProject.getName());

        EnvVar expectedEnvVar = new EnvVarBuilder().withName(KubernetesLauncher.GEBIT_BUILD_CLUSTER_WORKSPACE_PATH).withValue("maven-cloud-project").build();
        assertEquals("Container has wrong GEBIT_BUILD_CLUSTER_WORKSPACE_PATH", expectedEnvVar, pod.getSpec().getContainers().get(0).getEnv().get(0));

    }

    @Test
    public void testaddPodLabeAndlJnlpEnvVarsMatrixProject() {
        Collection<MatrixConfiguration> configurations = matrixProject.getActiveConfigurations();
        for (MatrixConfiguration configuration : configurations) {
            Pod pod = KubernetesCloudLimiterTest.createTestPod("jnlp", "node1", "1", "2", "1Gi", "2Gi", "running", "matrix-cloud-project");
            Queue.BuildableItem item = new Queue.BuildableItem(new Queue.WaitingItem(Calendar.getInstance(), configuration, new ArrayList<>()));

            String namespace = "pos";
            launcher.addPodLabeAndlJnlpEnvVars(pod, item, namespace);

            String expectedPodLabel = matrixProject.getName() + "-" + configuration.getCombination().toString('-', '-');
            assertEquals("Pod has wrong JOB_NAME_LABEL", pod.getMetadata().getLabels().get(KubernetesLauncher.JOB_NAME_LABEL), expectedPodLabel);

            String expectedWorkspacePath = matrixProject.getName() + "/" + configuration.getCombination().toString('/', '/');
            EnvVar expectedEnvVar = new EnvVarBuilder().withName(KubernetesLauncher.GEBIT_BUILD_CLUSTER_WORKSPACE_PATH).withValue(expectedWorkspacePath).build();
            assertEquals("Container has wrong GEBIT_BUILD_CLUSTER_WORKSPACE_PATH", expectedEnvVar, pod.getSpec().getContainers().get(0).getEnv().get(0));
        }
    }

    @Test
    public void testaddPodLabeAndlJnlpEnvVarsPipelineProject() {
        //TODO: implement
    }
}

