Feature: replica_availability

  Scenario: Calling the replica_availability command on a cluster with unavailable replicas
     Given we have an existing kafka cluster with a topic
      when we call the replica_availability command
      then OK replica_availability will be printed
