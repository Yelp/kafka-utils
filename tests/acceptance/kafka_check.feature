Feature: kafka_check

  Scenario: Calling the min_isr command on a cluster with isr greater or equal to min.isr for each topic
     Given we have an existing kafka cluster with a topic
      when we change min.isr settings for a topic to 1
      when we call the min_isr command
      then OK will be printed

  Scenario: Calling the min_isr command on a cluster with isr smaller than min.isr
     Given we have an existing kafka cluster with a topic
      when we change min.isr settings for a topic to 2
      when we call the min_isr command
      then CRITICAL will be printed
