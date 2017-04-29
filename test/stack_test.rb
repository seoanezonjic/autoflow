require File.dirname(__FILE__) + '/test_helper.rb'

class StackTest < Test::Unit::TestCase

  def setup

  end

  def test_scan_nodes
    stack = Stack.new('exec', {
      :cpu => 16, 
      :mem => '4gb', 
      :time => '20:00:00',
      :node =>  nil,
      :multinode => 0,
      :ntask => FALSE,
      :key_name => FALSE,
      :retry => FALSE,
      :Variables => nil,
      :workflow => ''
      })

    # test simple node
    #---------------------------------------------

    node_lines = [
      #Single node
      "result){\n",
        "null\n", 
        "?\n", 
        "touch algo\n", 
      "}\n"
    ]
    
    result = [
      ["result)", "null\n", "touch algo\n"]
    ]
    
    test = stack.scan_nodes(node_lines)
    assert_equal(result, test)

    # test double node
    #---------------------------------------------

    node_lines = [
      #Single node
      "algo){\n", 
        "null\n", 
        "?\n", 
        "echo 'OK'\n", 
      "}\n",
      #Single node 
      "result){\n", 
        "null\n", 
        "?\n", 
        "touch algo\n", 
      "}\n"
    ]
    
    result = [
      ["algo)", "null\n", "echo 'OK'\n"], 
      ["result)", "null\n", "touch algo\n"]
    ]
    
    test = stack.scan_nodes(node_lines)
    assert_equal(result, test)

    # test nested iterative node
    #---------------------------------------------

    node_lines = [
      #Nested iterative nodes
      "itera_[11;22]){\n", 
        "algo_[aa;bb]){\n", 
          "null\n", 
          "?\n", 
          "echo 'OK'itera_(+)(*)\n", 
        "}\n", 
      "}\n"
    ]
    
    result = [
      ["itera_[11;22])", "", [1]], 
      ["algo_[aa;bb])", "null\n", "echo 'OK'itera_(+)(*)\n"]
    ]
    
    test = stack.scan_nodes(node_lines)
    assert_equal(result, test)

    # test nested 3 iterative node
    #---------------------------------------------

    node_lines = [
      #Nested iterative nodes
      "itera_[11;22]){\n", 
        "?\n", 
        "algo_[aa;bb]){\n", 
         "?\n", 
          "mas_[ZZ;YY]){\n", 
            "null\n", 
            "?\n", 
            "echo 'OK'itera_(+)(*)mas_(+)\n", 
          "}\n", 
        "}\n", 
      "}\n"
    ]
    
    result = [
      ["itera_[11;22])", "", [1]], 
      ["algo_[aa;bb])", "", [2]], 
      ["mas_[ZZ;YY])", "null\n", "echo 'OK'itera_(+)(*)mas_(+)\n"]
    ]
    
    test = stack.scan_nodes(node_lines)
    assert_equal(result, test)

    #test nested iterative nodes with a final simgle node (bug)
    #--------------------------------------------------------------

    node_lines = [
      #Nested iterative nodes
      "itera_[11;22]){\n", 
        "algo_[aa;bb]){\n", 
          "null\n", 
          "?\n", 
          "echo 'OK'itera_(+)(*)\n", 
        "}\n", 
      "}\n", 
      #Single node
      "result){\n", 
        "null\n", 
        "?\n", 
        "touch algo\n", 
      "}\n"
    ]
    
    result = [
      ["itera_[11;22])", "", [1]], 
      ["algo_[aa;bb])", "null\n", "echo 'OK'itera_(+)(*)\n"], 
      ["result)", "null\n", "touch algo\n"]
    ] 
    
    test = stack.scan_nodes(node_lines)
    assert_equal(result, test)
  end
  
    
end
