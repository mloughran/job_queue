require File.dirname(__FILE__) + '/spec_helper'
require File.dirname(__FILE__) + '/common_adapter_spec'

describe JobQueue::TestAdapter do
  before :all do
    JobQueue.adapter = JobQueue::TestAdapter.new
  end
  
  it_should_behave_like 'JobQueue adapter basic'
  
  it_should_behave_like "JobQueue adapter named queues"
  
  it "should allow queue inspection as a hash" do
    JobQueue.adapter.queue.should == []
    JobQueue.put('hello')
    JobQueue.adapter.queue.should == ['hello']
  end
end
