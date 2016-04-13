def assignment_to_plan(assignment):
    """Convert an assignment to the format used by Kafka to
    describe a reassignment plan.
    """
    return {
        'version': 1,
        'partitions':
        [{'topic': t_p[0],
          'partition': t_p[1],
          'replicas': replica
          } for t_p, replica in assignment.iteritems()]
    }
