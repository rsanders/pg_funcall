lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

if ENV['COVERAGE'] == 'true'
  require 'simplecov'
  SimpleCov.start
end

require 'yaml'
require 'active_record'
configs = YAML.load(File.read("config/database.yml.example"))
ActiveRecord::Base.establish_connection(configs['test'])

require 'pg_funcall'

begin
  require 'pry'
rescue LoadError
  #
end
