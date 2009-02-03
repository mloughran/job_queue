require 'beanstalk-client'

class JobQueue::BeanstalkAdapter
  def initialize
    @beanstalk = Beanstalk::Pool.new(['localhost:11300'])
  end
  
  def put(string)
    @beanstalk.put(string)
  end
  
  def subscribe(&block)
    loop do
      begin
        job = @beanstalk.reserve
        puts "Beanstalkd received #{job.body}"
        yield job.body
        job.delete
      rescue => e
        # TODO: Improve error logging
        puts "Job failed: #{e.message}"
      end
    end
  end
end
