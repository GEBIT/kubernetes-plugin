#!/bin/bash

mvn -Dset.changelist \
  -DaltDeploymentRepository=gebit-releases::default::https://gebit-nexus.local.gebit.de/content/repositories/gebit-releases \
  clean deploy
