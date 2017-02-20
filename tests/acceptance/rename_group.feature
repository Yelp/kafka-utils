Feature: kafka_consumer_manager rename_group subcommand

  Scenario: Calling the rename_group commandi with default storage
     Given we have an existing kafka cluster with a topic
      when we produce some number of messages into the topic
      when we consume some number of messages from the topic
      when we call the rename_group command
      then the committed offsets in the new group will match the expected values
	
  @kafka_offset_storage 
  Scenario: Calling the rename_group command with kafka storage
     Given we have an existing kafka cluster with a topic
     Given we have initialized kafka offsets storage		
     Given we have a kafka consumer group with storage option kafka
       when we call the rename_group command with kafka storage
       then the group named has been changed
