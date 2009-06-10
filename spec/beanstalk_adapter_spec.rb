require File.dirname(__FILE__) + '/spec_helper'

describe JobQueue::BeanstalkAdapter do
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
  
  describe "when connecting to one instance" do
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

  describe "when connecting to multiple instances" do
    before :all do
      JobQueue.adapter = JobQueue::BeanstalkAdapter.new({
        :hosts => ['localhost:10001', 'localhost:10002']
      })
    end

    it "should be possible to put jobs" do
      JobQueue.put('test')
      JobQueue.subscribe do |job|
        job.should == 'test'
        throw :stop
      end
    end

    it "should be possible to retrieve all jobs supplied" do
      # Put some jobs on the queue
      jobs = []
      (1..10).each do |i|
        body = i
        JobQueue.put("#{body}")
        jobs << body
      end

      lambda {
        Timeout::timeout(3.5) do
          JobQueue.subscribe do |job|
            jobs.delete job.to_i
            throw :stop if jobs.empty?
          end
        end
      }.should_not raise_error(Timeout::Error)
    end

    it "should not log any errors when reserve times out" do
      JobQueue.logger.should_not_receive(:error)
      begin
        Timeout::timeout(1.5) do
          JobQueue.subscribe { |job| }
        end
      rescue Timeout::Error
        #Do nothing - timeout expected
      end
    end
  end
end
