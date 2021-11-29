package org.csanchez.jenkins.plugins.kubernetes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.kohsuke.accmod.Restricted;
import org.kohsuke.accmod.restrictions.NoExternalUse;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.umd.cs.findbugs.annotations.NonNull;
import hudson.Extension;
import hudson.ExtensionList;
import hudson.init.InitMilestone;
import hudson.init.Initializer;
import hudson.model.Node;
import hudson.model.Queue;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionRequest;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionReview;
import io.fabric8.kubernetes.api.model.admission.v1.AdmissionReviewBuilder;
import jenkins.metrics.api.Metrics;
import jenkins.model.Jenkins;
import jenkins.model.NodeListener;

/**
 * Implements provisioning limits for clouds and pod templates
 */
@Extension
public final class KubernetesProvisioningLimits {

    private static final Logger LOGGER = Logger.getLogger(KubernetesProvisioningLimits.class.getName());
    
    private static final String BC_LIMIT_URL = "http://bc-limit.bc-limit.svc/validate/pods";

    /**
     * Tracks current number of kubernetes agents per pod template
     */
    private final Map<String, Integer> podTemplateCounts = new HashMap<>();

    /**
     * Tracks current number of kubernetes agents per kubernetes cloud
     */
    private final Map<String, Integer> cloudCounts = new HashMap<>();

    @Initializer(after = InitMilestone.SYSTEM_CONFIG_LOADED)
    public static void init() {
        // We don't want anything to be provisioned while we do the initial count.
        Queue.withLock(() -> {
            final KubernetesProvisioningLimits instance = get();
            synchronized(instance) {
                Jenkins.get().getNodes()
                        .stream()
                        .filter(KubernetesSlave.class::isInstance)
                        .map(KubernetesSlave.class::cast)
                        .forEach(node -> {
                    instance.cloudCounts.put(node.getCloudName(), instance.getGlobalCount(node.getCloudName()) + node.getNumExecutors());
                    instance.podTemplateCounts.put(node.getTemplateId(), instance.getPodTemplateCount(node.getTemplateId()) + node.getNumExecutors());
                });
            }
        });
    }

    /**
     * @return the singleton instance
     */
    public static KubernetesProvisioningLimits get() {
        return ExtensionList.lookupSingleton(KubernetesProvisioningLimits.class);
    }

    /**
     * Register executors
     * @param cloud the kubernetes cloud the executors will be on
     * @param podTemplate the pod template used to schedule the agent
     * @param numExecutors the number of executors (pretty much always 1)
     */
    public synchronized boolean register(@NonNull KubernetesCloud cloud, @NonNull PodTemplate podTemplate, int numExecutors) {
        int newGlobalCount = getGlobalCount(cloud.name) + numExecutors;
        if (newGlobalCount <= cloud.getContainerCap()) {
            int newPodTemplateCount = getPodTemplateCount(podTemplate.getId()) + numExecutors;
            if (newPodTemplateCount <= podTemplate.getInstanceCap()) {
                boolean admitted = admitPod(podTemplate);
                if (!admitted) {
                    LOGGER.log(Level.FINEST, () -> "pod was denied");
                    return false;
                }
                
                cloudCounts.put(cloud.name, newGlobalCount);
                LOGGER.log(Level.FINEST, () -> cloud.name + " global limit: " + newGlobalCount + "/" + cloud.getContainerCap());

                podTemplateCounts.put(podTemplate.getId(), newPodTemplateCount);
                LOGGER.log(Level.FINEST, () -> podTemplate.getName() + " template limit: " + newPodTemplateCount + "/" + podTemplate.getInstanceCap());
                return true;
            } else {
                LOGGER.log(Level.FINEST, () -> podTemplate.getName() + " template limit reached: " + getPodTemplateCount(podTemplate.getId()) + "/" + podTemplate.getInstanceCap() + ". Cannot add " + numExecutors + " more!");
                Metrics.metricRegistry().counter(MetricNames.REACHED_POD_CAP).inc();
            }
        } else {
            LOGGER.log(Level.FINEST, () -> cloud.name + " global limit reached: " + getGlobalCount(cloud.name) + "/" + cloud.getContainerCap() + ". Cannot add " + numExecutors + " more!");
            Metrics.metricRegistry().counter(MetricNames.REACHED_GLOBAL_CAP).inc();
        }
        return false;
    }

    /**
     * Unregisters executors, when an agent is terminated
     * @param cloud the kubernetes cloud the executors were on
     * @param podTemplate the pod template used to schedule the agent
     * @param numExecutors the number of executors (pretty much always 1)
     */
    public synchronized void unregister(@NonNull KubernetesCloud cloud, @NonNull PodTemplate podTemplate, int numExecutors) {
        int newGlobalCount = getGlobalCount(cloud.name) - numExecutors;
        if (newGlobalCount < 0) {
            LOGGER.log(Level.WARNING, "Global count for " + cloud.name + " went below zero. There is likely a bug in kubernetes-plugin");
        }
        cloudCounts.put(cloud.name, Math.max(0, newGlobalCount));
        LOGGER.log(Level.FINEST, () -> cloud.name + " global limit: " + Math.max(0, newGlobalCount) + "/" + cloud.getContainerCap());

        int newPodTemplateCount = getPodTemplateCount(podTemplate.getId()) - numExecutors;
        if (newPodTemplateCount < 0) {
            LOGGER.log(Level.WARNING, "Pod template count for " + podTemplate.getName() + " went below zero. There is likely a bug in kubernetes-plugin");
        }
        podTemplateCounts.put(podTemplate.getId(), Math.max(0, newPodTemplateCount));
        LOGGER.log(Level.FINEST, () -> podTemplate.getName() + " template limit: " + Math.max(0, newPodTemplateCount) + "/" + podTemplate.getInstanceCap());
    }

    /**
     * Construct a minimal pod definition, containing the resource requests from the pod template.
     * 
     * @param podTemplate The podTemplate to construct a pod from.
     * 
     * @return The minimal pod definition needed for resource checking. This is NOT a full pod
     * definition as accepted by the k8s-apiserver, but contains enough information for the bc-limit
     * webhook to make its decision.
     */
    private Pod constructResourceCheckingPod(@NonNull PodTemplate podTemplate) {
        // create containers to hold resource request information
        List<Container> containers = new ArrayList<>();
        // go over all containers of the template
        for (ContainerTemplate containerTemplate : podTemplate.getContainers()) {
            // create needed builders
            ContainerBuilder conBuilder = new ContainerBuilder();
            ResourceRequirementsBuilder resBuilder = new ResourceRequirementsBuilder();

            // put resource request info from container template into container
            Map<String, Quantity> resourceRequests = new HashMap<>();
            resourceRequests.put("cpu", new Quantity(containerTemplate.getResourceRequestCpu()));
            resourceRequests.put("memory", new Quantity(containerTemplate.getResourceRequestMemory()));
            ResourceRequirements reqs = resBuilder.withRequests(resourceRequests).build();
            containers.add(conBuilder.withName(containerTemplate.getName()).withResources(reqs).build());
        }
        // add default jenkins slave label to pod, so the bc-limit webhook recognizes it
        Map<String, String> labels = new HashMap<>();
        labels.put("jenkins", "slave");
        PodBuilder builder = new PodBuilder();
        return builder.
            withNewMetadata().
                withLabels(labels).
            endMetadata().
            withNewSpec().
                withContainers(containers).
            endSpec().build();
    }
    
    private boolean admitPod(@NonNull PodTemplate podTemplate) {
        LOGGER.log(Level.INFO, () -> "checking pod admittance for template: " + podTemplate.getName());

        Pod pod = constructResourceCheckingPod(podTemplate);
        LOGGER.log(Level.FINEST, () -> "constructed resource checking pod: " + pod);

        // package pod into an AdmissionReview
        AdmissionReviewBuilder admBuilder = new AdmissionReviewBuilder();
        AdmissionRequest r = new AdmissionRequest();
        r.setUid(UUID.randomUUID().toString());
        r.setObject(pod);
        r.setOperation("CREATE");

        AdmissionReview requestReview = admBuilder.withRequest(r).build();
        try {
            // prepare http call to bc-limit webhook
            URL url = new URL(BC_LIMIT_URL);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json; utf-8");
            con.setRequestProperty("Accept", "application/json");
            con.setDoOutput(true);

            // serialize admission review containing pod into json
            ObjectMapper mapper = new ObjectMapper();
            String jsonInputString = mapper.writeValueAsString(requestReview);

            // write the json to the output stream of the http connection
            try (OutputStream os = con.getOutputStream()) {
                byte[] input = jsonInputString.getBytes("utf-8");
                os.write(input, 0, input.length);
            }
    
            // read answer from input stream of http connection
            try (BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream(), "utf-8"))) {
                // to be sure, read it line by line
                StringBuilder response = new StringBuilder();
                String responseLine = null;
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine.trim());
                }
                // deserialize response of bc-limit webhook into AdmissionReview
                // (now filled with a response object)
                AdmissionReview responseReview = mapper.readValue(response.toString(), AdmissionReview.class);
                // get the allowed boolean from the response
                boolean allowed = responseReview.getResponse().getAllowed();
                if (!allowed) {
                    // log out errors
                    LOGGER.log(Level.WARNING, () -> "pod was not admitted: " + responseReview.getResponse().getStatus().getMessage());
                }
                return allowed;
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, () -> "error while trying to admit pod for template: " + podTemplate.getName() + "\n" + e);
        }

        // in case of error (bc-limit not reachable etc), always return true, so the
        // cloud is not blocked
        return true;
    }
    
    @NonNull
    @Restricted(NoExternalUse.class)
    int getGlobalCount(String cloudName) {
        return cloudCounts.getOrDefault(cloudName, 0);
    }

    @NonNull
    @Restricted(NoExternalUse.class)
    int getPodTemplateCount(String podTemplate) {
        return podTemplateCounts.getOrDefault(podTemplate, 0);
    }

    @Extension
    public static class NodeListenerImpl extends NodeListener {
        @Override
        protected void onDeleted(@NonNull Node node) {
            if (node instanceof KubernetesSlave) {
                KubernetesSlave kubernetesNode = (KubernetesSlave) node;
                PodTemplate template = kubernetesNode.getTemplateOrNull();
                if (template != null) {
                    KubernetesProvisioningLimits.get().unregister(kubernetesNode.getKubernetesCloud(), template, node.getNumExecutors());
                }
            }
        }
    }

}
