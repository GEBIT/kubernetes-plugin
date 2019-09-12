package org.csanchez.jenkins.plugins.kubernetes;

import com.google.common.util.concurrent.Futures;
import hudson.model.Descriptor;
import hudson.slaves.NodeProvisioner;

import java.io.IOException;
import java.util.concurrent.Future;

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
        Future f;
        String nodeName = null;
        KubernetesSlave agent = null;
        try {
            agent = KubernetesSlave
                    .builder()
                    .podTemplate(cloud.getUnwrappedTemplate(t))
                    .cloud(cloud)
                    .build();
            nodeName = agent.getNodeName();
            f = Futures.immediateFuture(agent);
        } catch (IOException | Descriptor.FormException e) {
            f = Futures.immediateFailedFuture(e);
        }
        ProvisioningActivity.Id id = new ProvisioningActivity.Id(cloud.getDisplayName(), t.getName(), nodeName);
        if (agent != null) {
            agent.setId(id);
        }
        return new TrackedPlannedNode(id, getNumExecutors(), f);
    }
}
