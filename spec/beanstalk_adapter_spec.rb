require File.dirname(__FILE__) + '/spec_helper'

describe JobQueue::BeanstalkAdapter do
  before :each do
    system "beanstalkd -p 10001 -d"
    system "beanstalkd -p 10002 -d"
    system "beanstalkd -p 11300 -d"
  end

  after :each do
    system "killall beanstalkd"
  end

  describe '#new' do
    before(:each) do
      @pool = Beanstalk::Pool.new(['localhost:11300'])
    end

    it "should default to localhost:11300" do
      Beanstalk::Pool.should_receive(:new).with(['localhost:11300'], "default").and_return @pool
      JobQueue.adapter = JobQueue::BeanstalkAdapter.new
      JobQueue.put('test')
    end

    it "should accept one beanstalk instance" do
      Beanstalk::Pool.should_receive(:new).with(['12.34.56.78:12345'], 'default').and_return(@pool)
      JobQueue.adapter = JobQueue::BeanstalkAdapter.new(:hosts => '12.34.56.78:12345')
      JobQueue.put('test')
    end

    it "should allow multiple beanstalk instances" do
      Beanstalk::Pool.should_receive(:new).with([
        '12.34.56.78:12345',
        '87.65.43.21:54321'
      ], 'default').and_return(@pool)
      JobQueue.adapter = JobQueue::BeanstalkAdapter.new({
        :hosts => ['12.34.56.78:12345', '87.65.43.21:54321']
      })
      JobQueue.put('test')
    end
  end

  describe "put" do
    before :all do
      JobQueue.adapter = JobQueue::BeanstalkAdapter.new
    end

    it "should return the job id" do
      job_id = JobQueue.put("hello 1")
      job_id.should == "localhost:11300_1"
    end

    it "should assign job priority" do
      jobs = ["1","2","3"]
      JobQueue.put(jobs[2], :priority => 3)
      JobQueue.put(jobs[1], :priority => 2)
      JobQueue.put(jobs[0], :priority => 1)

      jobs_received = []
      should_not_timeout(0.5) {
        index = 0
        JobQueue.subscribe do |job_body|
          index += 1
          jobs_received << job_body
          throw :stop if index == 3
        end
      }

      jobs_received.should == jobs
    end

    it "should be able to retrieve job stats by id" do
      job_id = JobQueue.put("hello 1")
      job_id.should == "localhost:11300_1"
      JobQueue.put("hello 2")
      stats = JobQueue.job_stats("localhost:11300_1")

      stats["id"].should == 1
      stats["tube"].should == "default"
    end

    it "should raise error when no connections exist" do
      system "killall beanstalkd"
      lambda {
        JobQueue.put('test')
      }.should raise_error(JobQueue::NoConnectionAvailable)
    end

    it "should succeed when one connection fails" do
      JobQueue.adapter = JobQueue::BeanstalkAdapter.new({
        :hosts => ['localhost:10001', 'localhost:666']
      })
      10.times{ job_id = JobQueue.put("hello 1")}
    end

    it "should report and error and delete the job if a job times out" do
      job_id = JobQueue.put("job1", :ttr => 2)
      JobQueue.put('test')

      JobQueue.logger.should_receive(:warn).with("Job timed out")

      index = 0
      JobQueue.subscribe do |body|
        index += 1
        throw :stop if index == 2
        sleep 2.2
      end

      JobQueue.job_stats(job_id).should be_nil
    end
  end

  describe "subscribe" do
    before :all do
      JobQueue.adapter = JobQueue::BeanstalkAdapter.new
    end

    it "should delete a job once it has been succesfully excecuted" do
      job_id = JobQueue.put('testdeleted')
      JobQueue.put('foo')
      index = 0
      JobQueue.subscribe do |body|
        index += 1
        throw :stop if index == 2
      end
      JobQueue.job_stats(job_id).should be_nil
    end
  end

  describe "job_stats" do
    before :all do
      JobQueue.adapter = JobQueue::BeanstalkAdapter.new
    end

    it "should gracefully deal with jobs where connection no longer exists" do
      JobQueue.job_stats("localhost:11305_1").should be_nil
    end

    it "should gracefully deal with jobs where job doesn't exist" do
      JobQueue.job_stats("localhost:11300_1").should be_nil
    end
  end

  describe "when connecting to one instance" do
    before :all do
      JobQueue.adapter = JobQueue::BeanstalkAdapter.new
    end

    it "should write onto queue and fetch stuff back off" do
      JobQueue.put("hello")

      should_not_timeout {
        JobQueue.subscribe do |job|
          @job = job
          throw :stop
        end
      }

      @job.should == "hello"
    end

    it "should output message if error raised in job" do
      JobQueue.put("hello")
      JobQueue.put("hello2")

      JobQueue.logger.should_receive(:error).with(/Job failed\w*/)

      should_not_timeout {
        index = 0
        JobQueue.subscribe do |job|
          index +=1
          raise 'foo' if index == 1
          throw :stop
        end
      }
    end

    it "should use error_report block if supplied" do
      JobQueue.put("hello")
      JobQueue.put("hello2")

      error_report = Proc.new do |job, e|
        JobQueue.logger.error "Yikes that broke matey!"
      end

      JobQueue.logger.should_receive(:error).with("Yikes that broke matey!")

      should_not_timeout {
        index = 0
        JobQueue.subscribe(:error_report => error_report) do |job|
          index +=1
          raise 'foo' if index == 1
          throw :stop
        end
      }
    end

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
    
    # TODO: This test is brittle.
    it "should be possible to retrieve all jobs supplied" do
      # Put some jobs on the queue
      jobs = []
      (1..8).each do |i|
        body = i
        JobQueue.put("#{body}")
        jobs << body
      end

      should_not_timeout(3.5) {
        JobQueue.subscribe do |job|
          jobs.delete job.to_i
          throw :stop if jobs.empty?
        end
      }
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

def should_not_timeout(timeout = 0.1)
  lambda {
    Timeout.timeout(timeout) do
      yield
    end
  }.should_not raise_error(Timeout::Error)
end
