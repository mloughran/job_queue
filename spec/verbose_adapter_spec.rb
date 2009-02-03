require File.dirname(__FILE__) + '/spec_helper'

describe JobQueue::TestAdapter do
  before :all do
    JobQueue.adapter = JobQueue::TestAdapter.new
  end
  
  it "should write onto queue and output a very verbose message to stdout" do
    pending "How to write expectation on puts?"
    
    JobQueue.put("hello")
  end
end
