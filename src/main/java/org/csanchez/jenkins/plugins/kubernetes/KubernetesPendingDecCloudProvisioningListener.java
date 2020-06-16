package org.csanchez.jenkins.plugins.kubernetes;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jenkinsci.plugins.kubernetes.auth.KubernetesAuthException;

import hudson.Extension;
import hudson.model.Node;
import hudson.slaves.CloudProvisioningListener;
import hudson.slaves.NodeProvisioner.PlannedNode;

@Extension
public class KubernetesPendingDecCloudProvisioningListener extends CloudProvisioningListener {

    private static final Logger LOGGER = Logger.getLogger(KubernetesPendingDecCloudProvisioningListener.class.getName());

    @Override
    public void onCommit(PlannedNode plannedNode, Node node) {
        if (!(node instanceof KubernetesSlave)) {
            return;
        }

        KubernetesCloud kubernetesCloud = ((KubernetesSlave) node).getKubernetesCloud();
        onCompleteInternal(kubernetesCloud, (KubernetesSlave) node);
    }

    @Override
    public void onRollback(PlannedNode plannedNode, Node node, Throwable t) {
        onCommit(plannedNode, node);
    }

    private void onCompleteInternal(KubernetesCloud kubernetesCloud, KubernetesSlave node) {
        KubernetesCloudLimiter limiter = kubernetesCloud.getLimiter();

        try {
            limiter.acquireLock();
            limiter.decPending(node.getTemplate());
        } catch (InterruptedException | IOException | KubernetesAuthException e) {
            LOGGER.log(Level.SEVERE, "error acquiring lock for global config map", e);
            throw new IllegalStateException("error acquiring lock for global config map", e);
        } finally {
            try {
                limiter.releaseLock();
            } catch (IOException | KubernetesAuthException e) {
                LOGGER.log(Level.SEVERE, "error releasing lock for global config map", e);
                throw new IllegalStateException("error releasing lock for global config map", e);
            }
        }
    }
}
