class JobQueue::TestAdapter
  def initialize
    @queue = []
  end
  
  def put(string)
    @queue << string
  end
  
  def subscribe(&block)
    loop do
      sleep 0.1 if @queue.empty?
      yield @queue.shift
    end
  end
end
