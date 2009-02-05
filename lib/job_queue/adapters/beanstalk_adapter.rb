require 'beanstalk-client'

class JobQueue::BeanstalkAdapter
  def initialize
    @beanstalk = Beanstalk::Pool.new(['localhost:11300'])
  end
  
  def put(string)
    @beanstalk.put(string)
  end
  
  def subscribe(error_report, &block)
    loop do
      begin
        job = @beanstalk.reserve
        puts "Beanstalk received #{job.body}"
        yield job.body
        job.delete
      rescue => e
        error_report.call(job.body, e)
      end
    end
  end
end
