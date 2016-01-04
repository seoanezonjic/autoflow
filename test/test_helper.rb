require 'stringio'
require 'test/unit'

ROOT_PATH=File.dirname(__FILE__)
$: << File.expand_path(File.join(ROOT_PATH, "../lib/"))
$: << File.expand_path(File.join(ROOT_PATH, "../lib/autoflow/"))
$: << File.expand_path(File.join(ROOT_PATH, "../lib/autoflow/queue_managers"))

require File.dirname(__FILE__) + '/../lib/autoflow/program'
require File.dirname(__FILE__) + '/../lib/autoflow/batch'
require File.dirname(__FILE__) + '/../lib/autoflow/stack'
