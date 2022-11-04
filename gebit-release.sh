#!/bin/bash

CHANGELIST=`mvn -Dset.changelist validate | head -n 1 | awk '{ print $3 }'`
CHANGELIST=${CHANGELIST/-Dchangelist=/}

mvn -Dset.changelist \
  -DaltDeploymentRepository=gebit-releases::default::https://gebit-nexus.local.gebit.de/content/repositories/gebit-releases \
  clean install deploy

git tag $CHANGELIST
git push origin $CHANGELIST
