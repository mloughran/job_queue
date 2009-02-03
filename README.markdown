JobQueue
========

`job_queue` allows you to use lots of message queues with exactly the same interface so you don't need to worry about which queue to pick :)

This should get you started:

    require 'rubygems'
    require 'job_queue'

Before you can do anything you must specify an adapter to use

    JobQueue.adapter = JobQueue::BeanstalkdAdapter.new

Jobs can then be simply added to the queue

    JobQueue.put("flubble bubble")
    
In your workers you'll want to subscribe to a queue

    JobQueue.subscribe do |job|
      puts job
    end

This subscribe block takes care of waiting for the next job to arrive and the block is passed exactly what you passed in. If you want to exit the loop just throw :stop.

    JobQueue.subscribe do |job|
      # Wait - I changed my mind!
      throw :stop
    end

What should you put on the queue
--------------------------------

You might love Ruby right now, but why lock yourself in? Often the kinds of things you use queues for are the kind of things you'll want to optimize. This is a good place to start:

    JSON.generate({:some => "hash"})
    JSON.parse(job)

Can you show me a nice processing daemon?
-----------------------------------------

Yes. Just a minute...

Adapters
========

Take your pick! Right now we have:

Beanstalkd
----------
<http://xph.us/software/beanstalkd/>

AMQP
----
<http://github.com/tmm1/amqp/>

You need to run all your code within an eventmachine loop to use AMQP.
