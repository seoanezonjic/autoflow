class Program
	attr_accessor :name, :exec_folder_program, :parameters, :initialization, :queue_id, :done, :dependencies, :monocpu, :end_tasks_buffer
	def initialize(name, parameters, initialization, iterated, exec_folder_program, dependencies, done, end_tasks_buffer)
		@name=name
    		@parameters=parameters
		@initialization=initialization
		@exec_folder_program=exec_folder_program
		@iterated=iterated
		@queue_id=nil
		@done=done
		@dependencies=dependencies
		@end_tasks_buffer=end_tasks_buffer
		@monocpu=TRUE
		if @parameters =~ /\[cpu\]/
			@monocpu=FALSE
		end
	end

	def inspect
		string=@name.to_s+"\t"+@parameters.to_s+"\t"+@exec_folder_program.to_s+"\t"+"\t"+@iterated.to_s
	end
end
