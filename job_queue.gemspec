# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = %q{job_queue}
  s.version = "0.0.6"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Martyn Loughran"]
  s.date = %q{2009-06-12}
  s.description = %q{JobQueue means you don't have to worry about your queue any more!}
  s.email = %q{me@mloughran.com}
  s.files = ["README.markdown", "VERSION.yml", "lib/job_queue", "lib/job_queue/adapters", "lib/job_queue/adapters/amqp_adapter.rb", "lib/job_queue/adapters/beanstalk_adapter.rb", "lib/job_queue/adapters/test_adapter.rb", "lib/job_queue/adapters/verbose_adapter.rb", "lib/job_queue/job_queue.rb", "lib/job_queue.rb", "spec/amqp_adapter_spec.rb", "spec/beanstalk_adapter_spec.rb", "spec/job_queue_spec.rb", "spec/spec_helper.rb", "spec/test_adapter_spec.rb", "spec/verbose_adapter_spec.rb"]
  s.has_rdoc = true
  s.homepage = %q{http://github.com/mloughran/job_queue}
  s.rdoc_options = ["--inline-source", "--charset=UTF-8"]
  s.require_paths = ["lib"]
  s.rubygems_version = %q{1.3.1}
  s.summary = %q{JobQueue means you don't have to worry about your queue any more!}

  if s.respond_to? :specification_version then
    current_version = Gem::Specification::CURRENT_SPECIFICATION_VERSION
    s.specification_version = 2

    if Gem::Version.new(Gem::RubyGemsVersion) >= Gem::Version.new('1.2.0') then
    else
    end
  else
  end
end
