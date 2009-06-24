require File.dirname(__FILE__) + '/spec_helper'
require File.dirname(__FILE__) + '/common_adapter_spec'

describe JobQueue::BeanstalkAdapter do
  before :each do
    # On OSX we the -d flag doesn't work for beanstalk 1.3. This is a
    # workaround for that issue. We sleep a little to let processes start.
    system "beanstalkd -p 10001 &"
    system "beanstalkd -p 10002 &"
    system "beanstalkd -p 11300 &"
    sleep 0.1
  end

  after :each do
    system "killall beanstalkd"
  end

  describe '#new' do
    before(:each) do
      @pool = JobQueue::BeanstalkAdapter::BeanstalkPoolFix.new([
        'localhost:11300'
      ])
    end

    it "should default to localhost:11300" do
      JobQueue::BeanstalkAdapter::BeanstalkPoolFix.should_receive(:new).with(
        ['localhost:11300'],
        "default"
      ).and_return @pool
      JobQueue.adapter = JobQueue::BeanstalkAdapter.new
      JobQueue.put('test')
    end

    it "should accept one beanstalk instance" do
      JobQueue::BeanstalkAdapter::BeanstalkPoolFix.should_receive(:new).with(
        ['12.34.56.78:12345'],
        'default'
      ).and_return(@pool)
      JobQueue.adapter = JobQueue::BeanstalkAdapter.new(
        :hosts => '12.34.56.78:12345'
      )
      JobQueue.put('test')
    end

    it "should allow multiple beanstalk instances" do
      JobQueue::BeanstalkAdapter::BeanstalkPoolFix.should_receive(:new).with(
        ['12.34.56.78:12345', '87.65.43.21:54321'],
        'default'
      ).and_return(@pool)
      JobQueue.adapter = JobQueue::BeanstalkAdapter.new({
        :hosts => ['12.34.56.78:12345', '87.65.43.21:54321']
      })
      JobQueue.put('test')
    end
  end

  describe "put" do
    before :each do
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

    it "should raise an error if a ttr of < 2 is specified" do
      lambda {
        JobQueue.put('test', :ttr => 1.9)
      }.should raise_error(JobQueue::ArgumentError)

      lambda {
        JobQueue.put('test', :ttr => 2)
      }.should_not raise_error(JobQueue::ArgumentError)
    end
  end

  describe "subscribe" do
    before :each do
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

    it "should allow a client to cleanup if a job times out" do
      JobQueue.put('jobcleanup', :ttr => 2)
      JobQueue.put('test')

      cleanup = nil

      index = 0
      JobQueue.subscribe(:cleanup => lambda { |job| FileUtils.rm(job) }) do |body|
        file = File.open(body, 'w')
        file << "hello"
        file.flush

        index += 1
        throw :stop if index == 2
        sleep 2.2
      end

      File.exists?('jobcleanup').should be_false
    end

    # This test is for a patch that fixes a connection leaking issue in
    # beanstalk-client 1.0.2
    it "should not open more connections to beanstalk over time" do
      # Every 1.5 seconds, add a new job to the queue and check how many
      # connections are currently open according to beanstalkd.
      connections = []
      Thread.new do
        pool = Beanstalk::Pool.new(["localhost:11300"])
        loop do
          sleep 1.5
          JobQueue.put("job")
          connections << pool.stats["total-connections"]
        end
      end

      # Subscribe for 3 loops - gives time for a few timeouts to occur (1s)
      i = 0
      JobQueue.subscribe do |job|
        i += 1
        throw :stop if i == 3
      end

      # The number of connections should have been constant
      connections.uniq.size.should == 1
    end
  end

  describe "job_stats" do
    before :each do
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
    before :each do
      JobQueue.adapter = JobQueue::BeanstalkAdapter.new
    end

    it_should_behave_like "JobQueue adapter named queues"

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
  end

  describe "when connecting to multiple instances" do
    before :each do
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
