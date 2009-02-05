# JobQueue abstracts the task of adding work to a queue.
# 
# Beanstalk is fantastic, but maybe not "enterprise grade".
# 
# AMQP is fantastic, but it's bloody complex and has to run inside an
# eventmachine loop.
# 
# Take your pick!
# 
# Before use, an adapter must be chosen:
# 
#   JobQueue.adapter = JobQueue::BeanstalkAdapter.new
# 
# Jobs can then be simply added to the queue with
# 
#   JobQueue.put("flubble bubble")
# 
class JobQueue
  class << self
    attr_accessor :adapter
  end
  
  def self.put(string)
    adapter.put(string)
  end
  
  def self.subscribe(error_report = nil, &block)
    catch :stop do
      error_report ||= Proc.new do |job, e|
        puts \
          "Job failed\n" \
          "==========\n" \
          "Job content: #{job.inspect}\n" \
          "Exception: #{e.message}\n" \
          "#{e.backtrace.join("\n")}\n" \
          "\n"
      end
      
      adapter.subscribe(error_report, &block)
    end
  end
end
