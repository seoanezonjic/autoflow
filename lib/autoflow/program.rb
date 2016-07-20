class Program
	attr_accessor :name, :initialization, :parameters, :dependencies, :attrib, :queue_id, :batch

	def initialize(name, initialization, parameters, dependencies, job_attrib)
		@name = name
		@initialization = initialization
    	@parameters = parameters
		@dependencies = dependencies
		@attrib = job_attrib
		@queue_id = nil
		@batch = nil
	end

	def inspect
		if @parameters.class.to_s == 'String'
			program = @parameters.split(' ').first
			command = @parameters.gsub("\n","\n\t")
		else
			program = 'iterative_job'
			command = @parameters.map{|b| b}.join(' ')
		end
		string="\e[31m#{program}\n\e[0m\t\e[33m#{command}\e[0m\e[34m#{@attrib[:exec_folder]}\e[0m"
	end

end
