# This adapter is designed for testing purposes.
#
# Features supported:
#
# named queues: yes
# priority: no
# ttr: no
#
# Additionally this queue can be inspeced with JobQueue.adapter.queue('name')
#
class JobQueue::TestAdapter
  def initialize(options = {})
    @queues = {}
  end

  def put(string, queue, priority, ttr)
    @queues[queue] ||= []
    @queues[queue] << string
  end

  def subscribe(error_report, cleanup_task, queue, &block)
    loop do
      begin
        if get_queue(queue).empty?
          sleep 0.1
        else
          job = get_queue(queue).shift
          yield job
        end
      rescue => e
        error_report.call(job, e)
      end
    end
  end

  # Additional method for TestAdapter to allow easy queue inspection with
  #
  # JobQueue.adapter.queue('foo')
  #
  def queue(queue = 'default')
    get_queue(queue)
  end

  def queue_length(queue)
    @queues[queue].size
  end

  private

  def get_queue(queue)
    @queues[queue] || []
  end
end
