class JobQueue::TestAdapter
  def initialize(options = {})
    @queue = []
  end
  
  def put(string)
    @queue << string
  end
  
  def subscribe(error_report, &block)
    loop do
      begin
        sleep 0.1 if @queue.empty?
        yield @queue.shift
      rescue
        error_report.call(job.body, e)
      end
    end
  end
end
