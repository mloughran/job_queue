$TESTING=true
$:.push File.join(File.dirname(__FILE__), '..', 'lib')

require 'rubygems'
require 'job_queue'

def should_not_timeout(timeout = 0.1)
  lambda {
    Timeout.timeout(timeout) do
      yield
    end
  }.should_not raise_error(Timeout::Error)
end
