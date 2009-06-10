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
    attr_accessor :logger
    
    def logger
      @logger ||= begin
        logger = Logger.new(STDOUT)
        logger.level = Logger::WARN
        logger.debug("Created logger")
        logger
      end
    end
  end
  
  def self.put(string, options = {})
    queue = options[:queue] || 'default'
    priority = options[:priority] || 50
    adapter.put(string, queue, priority)
  end
  
  def self.subscribe(error_report = nil, options = {}, &block)
    queue = options[:queue] || 'default'
    catch :stop do
      error_report ||= Proc.new do |job_body, e|
        JobQueue.logger.error \
          "Job failed\n" \
          "==========\n" \
          "Job content: #{job_body.inspect}\n" \
          "Exception: #{e.message}\n" \
          "#{e.backtrace.join("\n")}\n" \
          "\n"
      end
      
      adapter.subscribe(error_report, queue, &block)
    end
  end
end
