package org.csanchez.jenkins.plugins.kubernetes;

import hudson.model.Descriptor;
import hudson.slaves.NodeProvisioner;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.jenkinsci.plugins.cloudstats.ProvisioningActivity;
import org.jenkinsci.plugins.cloudstats.TrackedPlannedNode;

/**
 * The tracked {@link PlannedNodeBuilder} implementation.
 */
public class TrackedPlannedNodeBuilder extends PlannedNodeBuilder {
    @Override
    public NodeProvisioner.PlannedNode build() {
        KubernetesCloud cloud = getCloud();
        PodTemplate t = getTemplate();
        CompletableFuture f;
        String nodeName = null;
        KubernetesSlave agent = null;
        try {
            agent = KubernetesSlave.builder()
                    .podTemplate(t.isUnwrapped() ? t : cloud.getUnwrappedTemplate(t))
                    .cloud(cloud)
                    .build();
            nodeName = agent.getNodeName();
            f = CompletableFuture.completedFuture(agent);
        } catch (IOException | Descriptor.FormException e) {
            f = new CompletableFuture();
            f.completeExceptionally(e);
        }
        ProvisioningActivity.Id id = new ProvisioningActivity.Id(cloud.getDisplayName(), t.getName(), nodeName);
        if (agent != null) {
            agent.setId(id);
        }
        return new TrackedPlannedNode(id, getNumExecutors(), f);
    }
}
