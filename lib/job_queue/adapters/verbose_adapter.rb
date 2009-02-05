# This isn't a queue at all, it just writes to standard output.
# 
# It might be useful for testing.
# 
class JobQueue::VerboseAdapter
  def initialize
    
  end
  
  def put(string)
    JobQueue.logger.debug "===== NEW JOB ADDED TO QUEUE ===="
    JobQueue.logger.debug string
    JobQueue.logger.debug "===== END OF MESSAGE ============"
  end
  
  def subscribe(error_report, &block)
    raise "Not implemented. Use a better adapter!!"
  end
end
