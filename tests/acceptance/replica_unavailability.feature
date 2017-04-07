Feature: replica_unavailability

  Scenario: Calling the replica_unavailability command on a cluster with unavailable replicas
     Given we have an existing kafka cluster with a topic
      when we call the replica_unavailability command
      then OK replica_unavailability will be printed
