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
end
