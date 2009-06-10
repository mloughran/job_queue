require 'beanstalk-client'

class JobQueue::BeanstalkAdapter
  def initialize(options = {})
    hosts = options[:hosts] || 'localhost:11300'
    @beanstalk = Beanstalk::Pool.new([hosts].flatten)
  end
  
  def put(string)
    @beanstalk.put(string)
  end
  
  def subscribe(error_report, &block)
    loop do
      begin
        job = @beanstalk.reserve(1)
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
end
