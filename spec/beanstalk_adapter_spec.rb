require File.dirname(__FILE__) + '/spec_helper'

describe JobQueue::BeanstalkAdapter do
  before :all do
    JobQueue.adapter = JobQueue::BeanstalkAdapter.new
  end

  describe '#new' do
    it "should default to localhost:11300" do
      Beanstalk::Pool.should_receive(:new).with(['localhost:11300'])
      JobQueue::BeanstalkAdapter.new
    end

    it "should accept one beanstalk instance" do
      Beanstalk::Pool.should_receive(:new).with(['12.34.56.78:12345'])
      JobQueue::BeanstalkAdapter.new(:hosts => '12.34.56.78:12345')
    end
    
    it "should allow multiple beanstalk instances" do
      Beanstalk::Pool.should_receive(:new).with([
        '12.34.56.78:12345',
        '87.65.43.21:54321'
      ])
      JobQueue::BeanstalkAdapter.new({
        :hosts => ['12.34.56.78:12345', '87.65.43.21:54321']
      })
    end
  end

  it "should write onto queue and fetch stuff back off" do
    JobQueue.put("hello")
    
    JobQueue.subscribe do |job|
      @job = job
      throw :stop
    end
    
    @job.should == "hello"
  end
  
  it "should output message if error raised in job" do
    JobQueue.put("hello")
    
    JobQueue.logger.should_receive(:error).with(/Job failed\w*/)
    
    index = 0
    JobQueue.subscribe do |job|
      index +=1
      raise 'foo' if index == 1
      throw :stop
    end
  end
  
  it "should use error_report block if supplied" do
    JobQueue.put("hello")
    
    error_report = Proc.new do |job, e|
      JobQueue.logger.error "Yikes that broke matey!"
    end
    
    JobQueue.logger.should_receive(:error).with("Yikes that broke matey!")
    
    index = 0
    JobQueue.subscribe(error_report) do |job|
      index +=1
      raise 'foo' if index == 1
      throw :stop
    end
  end
end
