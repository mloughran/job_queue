require File.dirname(__FILE__) + '/spec_helper'

describe JobQueue::BeanstalkAdapter do
  before :all do
    JobQueue.adapter = JobQueue::BeanstalkAdapter.new
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
    pending "use proper logger to assert things against"
    JobQueue.put("hello")
    
    index = 0
    JobQueue.subscribe do |job|
      index +=1
      raise 'foo' if index == 1
      throw :stop
    end
  end
  
  it "should use error_report block if supplied" do
    pending "use proper logger to assert things against"
    JobQueue.put("hello")
    
    error_report = Proc.new do |e|
      puts "Yikes that broke matey!"
    end
    
    index = 0
    JobQueue.subscribe(error_report) do |job|
      index +=1
      raise 'foo' if index == 1
      throw :stop
    end
  end
end
