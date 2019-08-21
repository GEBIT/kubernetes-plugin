package org.csanchez.jenkins.plugins.kubernetes;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;

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
        onCompleteInternal(kubernetesCloud);
    }

    @Override
    public void onRollback(PlannedNode plannedNode, Node node, Throwable t) {
        onCommit(plannedNode, node);
    }

    private void onCompleteInternal(KubernetesCloud kubernetesCloud) {
        KubernetesCloudLimiter limiter = kubernetesCloud.getLimiter();

        try {
            limiter.acquireLock();

            int numPendingLaunches = limiter.getNumOfPendingLaunchesK8S();
            numPendingLaunches--;
            limiter.setNumOfPendingLaunchesK8S(numPendingLaunches);

        } catch (InterruptedException | UnrecoverableKeyException | CertificateEncodingException | NoSuchAlgorithmException | KeyStoreException | IOException e) {
            LOGGER.log(Level.SEVERE, "error acquiring lock for global config map", e);
            throw new IllegalStateException("error acquiring lock for global config map", e);
        } finally {
            try {
                limiter.releaseLock();
            } catch (UnrecoverableKeyException | CertificateEncodingException | NoSuchAlgorithmException
                    | KeyStoreException | IOException e) {
                LOGGER.log(Level.SEVERE, "error releasing lock for global config map", e);
                throw new IllegalStateException("error releasing lock for global config map", e);
            }
        }
    }
}
