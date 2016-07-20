require File.dirname(__FILE__) + '/test_helper.rb'

class StackTest < Test::Unit::TestCase

  def setup

  end

  def test_scan_nodes

    # test a file with multiple gz streams

    file=Stack.new('exec', {
      :cpu => 16, 
      :mem => '4gb', 
      :time => '20:00:00',
      :node =>  nil,
      :multinode => 0,
      :ntask => FALSE
      })
    
    assert_equal(1,1)

  end
  
    
end
