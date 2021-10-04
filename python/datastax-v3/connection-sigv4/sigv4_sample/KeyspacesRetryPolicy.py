try:
    from cassandra.policies import RetryPolicy
except ImportError:
    raise RuntimeError('Required packages Failed To install please run "python Setup.py install" command or install using pip')

class KeyspacesRetryPolicy(RetryPolicy):
     def __init__(self, RETRY_MAX_ATTEMPTS=3):
          self.RETRY_MAX_ATTEMPTS = RETRY_MAX_ATTEMPTS

     def on_read_timeout ( self, query, consistency, required_responses, received_responses, data_retrieved, retry_num):
        # print ("query: " , query, "retry_num: "  , retry_num )
        if retry_num <= self.RETRY_MAX_ATTEMPTS:
            return self.RETRY, consistency
        else:
            return self.RETHROW, None 

     def on_write_timeout (self, query, consistency, write_type, required_responses, received_responses, retry_num):
        # print ("query: " , query, "retry_num: "  , retry_num )
        if retry_num <= self.RETRY_MAX_ATTEMPTS:
            return self.RETRY, consistency
        else:
            return self.RETHROW, None

     def on_unavailable (self, query, consistency, required_replicas, alive_replicas, retry_num):
        if retry_num <= self.RETRY_MAX_ATTEMPTS:
            return self.RETRY, consistency
        else:
            return self.RETHROW, None 

     def on_request_error (self, query, consistency, error, retry_num):
        if retry_num <= self.RETRY_MAX_ATTEMPTS:
            return self.RETRY, consistency
        else:
            return self.RETHROW, None 