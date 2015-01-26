# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'pg_funcall/version'

Gem::Specification.new do |spec|
  spec.name          = "pg_funcall"
  spec.version       = PgFuncall::VERSION
  spec.authors       = ["Robert Sanders"]
  spec.email         = ["robert@curioussquid.com"]
  spec.summary       = %q{Utility class for calling functions defined in a PostgreSQL database.}
  # spec.description   = %q{.}
  spec.homepage      = "http://github.com/rsanders/pg_funcall"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_dependency "pg", ">= 0.17.0"
  spec.add_dependency "activerecord", ">= 4.0.0"

  # support for various PG types
  spec.add_dependency "uuid"
  spec.add_dependency "ipaddr_extensions"

  spec.add_development_dependency "bundler", "~> 1.7"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec", "~> 2.14.0"
  spec.add_development_dependency "simplecov"
  spec.add_development_dependency "wwtd"

end
