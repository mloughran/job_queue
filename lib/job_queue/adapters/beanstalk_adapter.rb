require 'beanstalk-client'

class JobQueue::BeanstalkAdapter
  def initialize(options = {})
    @hosts = options[:hosts] || 'localhost:11300'
  end
  
  def put(string, queue, priority)
    beanstalk_pool(queue).put(string)
  end
  
  def subscribe(error_report, queues, &block)
    pool = Beanstalk::Pool.new([@hosts].flatten)
    pool.watch([queues].flatten)
    loop do
      begin
        job = pool.reserve(1)
        JobQueue.logger.info "Beanstalk received #{job.body}"
        begin
          yield job.body
        rescue => e
          error_report.call(job.body, e)
          job.delete
        end
      rescue Beanstalk::TimedOut
        # Do nothing - retry to reseve (from another host?)
      end
    end
  end
  
  def beanstalk_pool(queue)
    @beanstalk_pools ||= {}
    @beanstalk_pools[queue] ||= begin
      Beanstalk::Pool.new([@hosts].flatten, queue)
    end
  end
end
