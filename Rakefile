require 'bundler/gem_tasks'
require 'bundler/setup'
require 'wwtd/tasks'
require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new(:spec) do |config|
   #config.rcov = true
end

task :default => :wwtd
