require 'mq'

class JobQueue::AMQPAdapter
  def initialize
    amq = MQ.new
    @exchange = amq.direct('photo', :durable => true)
    @queue = amq.queue('photo_worker', :durable => true)
    @queue.bind(@exchange)
  end
  
  def put(string)
    @queue.publish(string, :persistent => true)
  end
  
  def subscribe(&block)
    EM.add_periodic_timer(0) do
      begin
        @queue.pop do |header, body| 
          next unless body
          puts "AMQP received #{body}"
          yield body
        end
      rescue => e
        # TODO: Improve error logging
        puts "Job failed: #{e.message}"
      end
    end
  end
end
