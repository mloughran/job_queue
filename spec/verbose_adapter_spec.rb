require File.dirname(__FILE__) + '/spec_helper'

describe JobQueue::VerboseAdapter do
  before :all do
    JobQueue.adapter = JobQueue::VerboseAdapter.new
  end
  
  it "should write onto queue and output a very verbose message to stdout" do
    JobQueue.logger.should_receive(:debug).with("===== NEW JOB ADDED TO QUEUE ====")
    JobQueue.logger.should_receive(:debug).with("hello")
    JobQueue.logger.should_receive(:debug).with("===== END OF MESSAGE ============")
    
    JobQueue.put("hello")
  end
end
