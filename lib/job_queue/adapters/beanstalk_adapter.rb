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
        job = @beanstalk.reserve
        JobQueue.logger.info "Beanstalk received #{job.body}"
        yield job.body
        job.delete
      rescue => e
        error_report.call(job.body, e)
      end
    end
  end
end
