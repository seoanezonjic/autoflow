# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'autoflow/version'

Gem::Specification.new do |spec|
  spec.name          = "autoflow"
  spec.version       = Autoflow::VERSION
  spec.authors       = ["Pedro Seoane"]
  spec.email         = ["seoanezonjic@hotmail.com"]
  spec.description   = %q{"Autoflow makes easy to launch big pipelines on a queue system. Only works with SLURM & PBS"}
  spec.summary       = %q{"This gem take a pipeline and launch it on a queue system"}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_runtime_dependency 'net-ssh', '>= 2.8.0'
  spec.add_runtime_dependency 'git', '>= 0.8.1'
  spec.add_runtime_dependency 'win32console', '>= 1.3.2' if !ENV['OS'].nil? && ENV['OS'].downcase.include?('windows')
  spec.add_runtime_dependency 'colorize', '>= 0.7.3'
  spec.add_runtime_dependency 'terminal-table', '>= 2.0.0'
  spec.add_runtime_dependency 'openssl', '>= 2.2.0'
  spec.add_development_dependency "bundler", ">= 2.2.7"
  spec.add_development_dependency "rake"
end
