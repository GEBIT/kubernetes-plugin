package org.csanchez.jenkins.plugins.kubernetes;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jenkinsci.plugins.kubernetes.auth.KubernetesAuthException;

import hudson.Extension;
import hudson.model.Label;
import hudson.model.queue.CauseOfBlockage;
import hudson.slaves.Cloud;
import hudson.slaves.CloudProvisioningListener;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.fabric8.kubernetes.client.KubernetesClient;

@Extension
public class KubernetesOverloadCloudProvisioningListener extends CloudProvisioningListener {

    private static final Logger LOGGER = Logger.getLogger(KubernetesOverloadCloudProvisioningListener.class.getName());

    @Override
    public CauseOfBlockage canProvision(Cloud cloud, Label label, int numExecutors) {
        LOGGER.log(Level.FINE, "check provision for cloud: {0}, label: {1}, numExecutors: {2}", new Object[] {cloud, label, numExecutors});

        if (!(cloud instanceof KubernetesCloud) || label == null) {
            //we handle KubernetesCloud and labels only
            return null;
        }

        LOGGER.log(Level.FINE, "current nodes for label {0}: {1}", new Object[] {label.getName(), label.getNodes()});

        KubernetesCloud kubernetesCloud = (KubernetesCloud) cloud;
        KubernetesClient client = null;
        try {
            client = kubernetesCloud.connect();
            List<Pod> pendingAgentPods = client.pods().inAnyNamespace().withLabel("jenkins", "slave").withField("status.phase", "Pending").list().getItems();
            LOGGER.log(Level.FINE, "found {0} pending pods", pendingAgentPods.size());
            for (Pod pendingPod : pendingAgentPods) {
                List<PodCondition> conditions = pendingPod.getStatus().getConditions();
                for (PodCondition condition : conditions) {
                    LOGGER.log(Level.FINE, "pod {0} has condition {1}", new Object[] {pendingPod.getMetadata().getName(), condition});
                    if ("Unschedulable".equals(condition.getReason()) && condition.getMessage().contains("Insufficient")) {
                        //there is at least one pod that can't be scheduled because of insufficient CPU, we can't provision any more agent pods
                        LOGGER.log(Level.FINE, "pod {0} can not be scheduled because of insufficient cpu or memory, cluster is busy", pendingPod.getMetadata().getName());
                        return new CauseOfBlockage.BecauseLabelIsBusy(label);
                    }
                }
            }
        } catch (IOException | KubernetesAuthException e) {
            LOGGER.log(Level.SEVERE, "error connecting to kubernetes", e);
            return new CauseOfBlockage.BecauseLabelIsOffline(label);
        }

        //yes, we can provision, there is no overload
        LOGGER.log(Level.FINE, "no overload detected");
        return null;
    }
}
