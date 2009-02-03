require File.dirname(__FILE__) + '/spec_helper'

describe JobQueue::TestAdapter do
  before :all do
    JobQueue.adapter = JobQueue::TestAdapter.new
  end
  
  it "should write onto queue and fetch stuff back off" do
    JobQueue.put("hello")
    
    JobQueue.subscribe do |job|
      @job = job
      throw :stop
    end
    
    @job.should == "hello"
  end
  
  it "should pull items off in the order the were added" do
    JobQueue.put("foo")
    JobQueue.put("bar")
    
    retrieved_jobs = []
    
    begin
      Timeout::timeout(0.5) do
        JobQueue.subscribe do |job|
          retrieved_jobs << job
        end
      end
    rescue Timeout::Error
      
    end
    
    retrieved_jobs[0].should == "foo"
    retrieved_jobs[1].should == "bar"
    retrieved_jobs[2].should == nil
  end
end
