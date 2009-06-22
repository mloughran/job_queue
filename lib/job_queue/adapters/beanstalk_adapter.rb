require 'beanstalk-client'
require 'timeout'

class JobQueue::BeanstalkAdapter
  def initialize(options = {})
    @hosts = options[:hosts] || 'localhost:11300'
  end

  def put(string, queue, priority, ttr)
    ttr = ttr.floor #rounding because Beanstalk doesnt accept float numbers
    raise JobQueue::ArgumentError, "TTR must be greater than 1" if ttr < 2

    delay = 0
    job_info = beanstalk_pool(queue).put_and_report_conn \
      string, priority, delay, ttr
    "#{job_info[:host]}_#{job_info[:id]}"
  rescue Beanstalk::NotConnected
    raise JobQueue::NoConnectionAvailable
  end

  def subscribe(error_report, cleanup_task, queue, &block)
    pool = Beanstalk::Pool.new([@hosts].flatten, queue)
    loop do
      begin
        job = pool.reserve(1)
        time_left = job.stats["time-left"]
        JobQueue.logger.info "Beanstalk received #{job.body}"
        Timeout::timeout([time_left - 1, 1].max) do
          yield job.body
        end
        job.delete
      rescue Timeout::Error
        cleanup_task.call(job.body)
        JobQueue.logger.warn "Job timed out"
        begin
          job.delete
        rescue Beanstalk::NotFoundError
          JobQueue.logger.error "Job timed out and could not be deleted"
        end
      rescue Beanstalk::TimedOut
        # Do nothing - retry to reseve (from another host?)
      rescue => e
        if job
          error_report.call(job.body, e)
          begin
            job.delete
          rescue Beanstalk::NotFoundError
            JobQueue.logger.error "Job failed but could not be deleted"
          end
        else
          JobQueue.logger.error "Unhandled exception: #{e.message}\n" \
            "#{e.backtrace.join("\n")}\n"
        end
      end
    end
  end

  def job_stats(job_id)
    host, id = job_id.split('_')
    beanstalk_pool.job_stats(id).select { |k, v| k == host }[0][1]
  rescue Beanstalk::NotFoundError
    nil
  end

  def beanstalk_pool(queue='default')
    @beanstalk_pools ||= {}
    @beanstalk_pools[queue] ||= begin
      Beanstalk::Pool.new([@hosts].flatten, queue)
    end
  end

  module BeanstalkExtension
    def put_and_report_conn(body, pri=65536, delay=0, ttr=120)
      send_to_rand_conn_and_report(:put, body, pri, delay, ttr)
    end

    def send_to_rand_conn_and_report(*args)
      connect()
      retry_wrap{
        conn = pick_connection
        {:host => conn.addr, :id => call_wrap(conn, *args)}
      }
    end

    def job_stats(id)
      make_hash(send_to_all_conns(:job_stats, id))
    end
  end
end

Beanstalk::Pool.send(:include, JobQueue::BeanstalkAdapter::BeanstalkExtension)
