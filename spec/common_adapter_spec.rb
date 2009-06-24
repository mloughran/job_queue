require File.dirname(__FILE__) + '/spec_helper'

shared_examples_for 'JobQueue adapter basic' do
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

shared_examples_for "JobQueue adapter named queues" do
  it "should put jobs onto a named queue and only read off that queue" do
    JobQueue.put("hello", :queue => "test")
    lambda {
      Timeout.timeout(0.1) do
        JobQueue.subscribe(:queue => "foo") do |job|
          throw :stop
        end
      end
    }.should raise_error(Timeout::Error)
    should_not_timeout {
      JobQueue.subscribe(:queue => "test") do |body|
        body.should == 'hello'
        throw :stop
      end
    }
  end
end